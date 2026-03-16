import secrets
import bcrypt
from flask import Blueprint, request, jsonify, redirect
from flask_jwt_extended import create_access_token, create_refresh_token, jwt_required, get_jwt_identity
from db import db
from models import User, PasswordResetToken
from email_utils import send_verification_email, send_reset_email
import os

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")
APP_URL = os.environ.get("APP_URL", "http://localhost:5000")


def _error(msg, code):
    return jsonify({"error": msg}), code


@auth_bp.route("/register", methods=["POST"])
def register():
    data     = request.get_json(silent=True) or {}
    email    = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not email or "@" not in email:
        return _error("Valid email required.", 400)
    if len(password) < 8:
        return _error("Password must be at least 8 characters.", 400)
    if User.query.filter_by(email=email).first():
        return _error("Email already registered.", 409)

    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    token  = secrets.token_urlsafe(32)
    user   = User(email=email, password_hash=hashed, verification_token=token)
    db.session.add(user)
    db.session.commit()

    send_verification_email(email, token)

    return jsonify({"message": "Registered successfully. Check your email to verify your account."}), 201


@auth_bp.route("/verify/<token>")
def verify_email(token):
    user = User.query.filter_by(verification_token=token).first()
    if not user:
        return redirect(f"{APP_URL}/app?verified=invalid")
    user.email_verified     = True
    user.verification_token = None
    db.session.commit()
    return redirect(f"{APP_URL}/app?verified=1")


@auth_bp.route("/resend-verification", methods=["POST"])
@jwt_required()
def resend_verification():
    user_id = get_jwt_identity()
    user    = User.query.get(int(user_id))
    if not user:
        return _error("User not found.", 404)
    if user.email_verified:
        return jsonify({"message": "Email already verified."}), 200
    if not user.verification_token:
        user.verification_token = secrets.token_urlsafe(32)
        db.session.commit()
    send_verification_email(user.email, user.verification_token)
    return jsonify({"message": "Verification email sent."}), 200


@auth_bp.route("/login", methods=["POST"])
def login():
    data     = request.get_json(silent=True) or {}
    email    = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    user = User.query.filter_by(email=email, is_active=True).first()
    if not user or not bcrypt.checkpw(password.encode(), user.password_hash.encode()):
        return _error("Invalid email or password.", 401)

    access_token  = create_access_token(identity=str(user.id))
    refresh_token = create_refresh_token(identity=str(user.id))
    return jsonify({
        "access_token":   access_token,
        "refresh_token":  refresh_token,
        "email_verified": user.email_verified,
    }), 200


@auth_bp.route("/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh():
    identity     = get_jwt_identity()
    access_token = create_access_token(identity=identity)
    return jsonify({"access_token": access_token}), 200


@auth_bp.route("/me")
@jwt_required()
def me():
    user_id = get_jwt_identity()
    user    = User.query.get(int(user_id))
    if not user:
        return _error("User not found.", 404)
    return jsonify({
        "email":          user.email,
        "email_verified": user.email_verified,
    }), 200


@auth_bp.route("/forgot-password", methods=["POST"])
def forgot_password():
    data  = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()

    # Always return 200 to avoid user enumeration
    user = User.query.filter_by(email=email, is_active=True).first()
    if user:
        # Invalidate previous tokens
        PasswordResetToken.query.filter_by(user_id=user.id, used=False).delete()
        db.session.commit()
        reset_token = PasswordResetToken.generate(user.id)
        db.session.add(reset_token)
        db.session.commit()
        send_reset_email(email, reset_token.token)

    return jsonify({"message": "If that email is registered, a reset link has been sent."}), 200


@auth_bp.route("/reset-password", methods=["POST"])
def reset_password():
    data     = request.get_json(silent=True) or {}
    token    = data.get("token") or ""
    password = data.get("password") or ""

    if len(password) < 8:
        return _error("Password must be at least 8 characters.", 400)

    reset = PasswordResetToken.query.filter_by(token=token).first()
    if not reset or not reset.is_valid():
        return _error("Invalid or expired reset link.", 400)

    user = User.query.get(reset.user_id)
    if not user:
        return _error("User not found.", 404)

    user.password_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    reset.used         = True
    db.session.commit()

    return jsonify({"message": "Password updated successfully."}), 200

import secrets
import hashlib
import bcrypt
from flask import Blueprint, request, jsonify, redirect
from flask_jwt_extended import create_access_token, create_refresh_token, jwt_required, get_jwt_identity
from db import db
from models import User, PasswordResetToken, UserAlert
from email_utils import send_verification_email, send_reset_email
import os

VALID_COMMODITIES = ["Gold", "Silver", "Crude Oil", "Copper", "Natural Gas"]

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
def resend_verification():
    """Public endpoint — takes email, resends verification if account exists and is unverified."""
    data  = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    # Always return 200 to avoid enumeration
    user = User.query.filter_by(email=email, is_active=True).first()
    if user and not user.email_verified:
        if not user.verification_token:
            user.verification_token = secrets.token_urlsafe(32)
            db.session.commit()
        send_verification_email(user.email, user.verification_token)
    return jsonify({"message": "If that email is registered and unverified, a new link has been sent."}), 200


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

    # Always return identical 200 — never reveal whether email exists
    SAFE = jsonify({"message": "If that email is registered, a reset link has been sent."}), 200

    user = User.query.filter_by(email=email, is_active=True).first()
    if not user:
        return SAFE

    # Invalidate all previous unused tokens for this user (prevent race)
    PasswordResetToken.query.filter_by(user_id=user.id, used=False).delete()
    db.session.flush()

    # Generate token — raw goes in email, only SHA-256 hash stored in DB
    raw_token, reset_instance = PasswordResetToken.create(
        user.id, created_by_ip=request.remote_addr
    )
    db.session.add(reset_instance)
    db.session.commit()
    send_reset_email(email, raw_token)

    return SAFE


@auth_bp.route("/change-email", methods=["POST"])
@jwt_required()
def change_email():
    user_id  = int(get_jwt_identity())
    data     = request.get_json(silent=True) or {}
    new_email = (data.get("email") or "").strip().lower()
    password  = data.get("password") or ""
    if not new_email or "@" not in new_email:
        return _error("Valid email required.", 400)
    user = User.query.get(user_id)
    if not user:
        return _error("User not found.", 404)
    if not bcrypt.checkpw(password.encode(), user.password_hash.encode()):
        return _error("Incorrect password.", 401)
    if User.query.filter_by(email=new_email).first():
        return _error("Email already in use.", 409)
    token = secrets.token_urlsafe(32)
    user.email              = new_email
    user.email_verified     = False
    user.verification_token = token
    db.session.commit()
    send_verification_email(new_email, token)
    return jsonify({"message": "Email updated. Check your new inbox to verify."}), 200


@auth_bp.route("/reset-password", methods=["POST"])
def reset_password():
    data        = request.get_json(silent=True) or {}
    raw_token   = data.get("token") or ""
    password    = data.get("password") or ""

    if len(password) < 8:
        return _error("Password must be at least 8 characters.", 400)

    # Try new hashed-token lookup first
    reset = None
    if len(raw_token) >= 8:
        token_hash   = hashlib.sha256(raw_token.encode()).hexdigest()
        token_prefix = raw_token[:8]
        reset = PasswordResetToken.query.filter_by(
            token_prefix=token_prefix,
            token_hash=token_hash,
            used=False,
        ).first()

    # Fall back to legacy plaintext lookup (covers tokens created before this fix)
    if not reset:
        reset = PasswordResetToken.query.filter_by(token=raw_token, used=False).first()

    if not reset or not reset.is_valid():
        return _error("Invalid or expired reset link.", 400)

    user = User.query.get(reset.user_id)
    if not user:
        return _error("Invalid or expired reset link.", 400)

    # Mark used BEFORE changing password (atomic-first pattern)
    reset.used       = True
    reset.used_by_ip = request.remote_addr
    db.session.flush()

    user.password_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    # Invalidate all existing sessions — any JWT issued before now is revoked
    from datetime import datetime, timezone
    user.tokens_valid_after = datetime.now(timezone.utc)

    db.session.commit()

    return jsonify({"message": "Password updated. Please sign in."}), 200


# ── Email alerts ───────────────────────────────────────────────────────────────
@auth_bp.route("/alerts", methods=["GET"])
@jwt_required()
def get_alerts():
    user_id = int(get_jwt_identity())
    alerts  = UserAlert.query.filter_by(user_id=user_id).all()
    # Return all commodities with their enabled state
    enabled_set = {a.commodity for a in alerts if a.enabled}
    return jsonify([
        {"commodity": c, "enabled": c in enabled_set}
        for c in VALID_COMMODITIES
    ])


@auth_bp.route("/notify", methods=["GET"])
@jwt_required()
def get_notify():
    user = User.query.get(int(get_jwt_identity()))
    if not user:
        return _error("User not found.", 404)
    return jsonify({"enabled": user.notify_on_analysis})


@auth_bp.route("/notify", methods=["POST"])
@jwt_required()
def set_notify():
    user = User.query.get(int(get_jwt_identity()))
    if not user:
        return _error("User not found.", 404)
    data = request.get_json(silent=True) or {}
    user.notify_on_analysis = bool(data.get("enabled", False))
    db.session.commit()
    return jsonify({"enabled": user.notify_on_analysis})


@auth_bp.route("/alerts", methods=["POST"])
@jwt_required()
def set_alert():
    user_id   = int(get_jwt_identity())
    data      = request.get_json(silent=True) or {}
    commodity = data.get("commodity")
    enabled   = bool(data.get("enabled", True))

    if commodity not in VALID_COMMODITIES:
        return _error("Invalid commodity.", 400)

    alert = UserAlert.query.filter_by(user_id=user_id, commodity=commodity).first()
    if alert:
        alert.enabled = enabled
    else:
        alert = UserAlert(user_id=user_id, commodity=commodity, enabled=enabled)
        db.session.add(alert)
    db.session.commit()
    return jsonify({"commodity": commodity, "enabled": enabled})

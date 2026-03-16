import bcrypt
from flask import Blueprint, request, jsonify
from flask_jwt_extended import create_access_token, create_refresh_token, jwt_required, get_jwt_identity
from db import db
from models import User

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")


def _error(msg, code):
    return jsonify({"error": msg}), code


@auth_bp.route("/register", methods=["POST"])
def register():
    data = request.get_json(silent=True) or {}
    email    = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not email or "@" not in email:
        return _error("Valid email required.", 400)
    if len(password) < 8:
        return _error("Password must be at least 8 characters.", 400)
    if User.query.filter_by(email=email).first():
        return _error("Email already registered.", 409)

    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    user = User(email=email, password_hash=hashed)
    db.session.add(user)
    db.session.commit()
    return jsonify({"message": "Registered successfully."}), 201


@auth_bp.route("/login", methods=["POST"])
def login():
    data = request.get_json(silent=True) or {}
    email    = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    user = User.query.filter_by(email=email, is_active=True).first()
    if not user or not bcrypt.checkpw(password.encode(), user.password_hash.encode()):
        return _error("Invalid email or password.", 401)

    access_token  = create_access_token(identity=str(user.id))
    refresh_token = create_refresh_token(identity=str(user.id))
    return jsonify({"access_token": access_token, "refresh_token": refresh_token}), 200


@auth_bp.route("/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh():
    identity     = get_jwt_identity()
    access_token = create_access_token(identity=identity)
    return jsonify({"access_token": access_token}), 200

import secrets
import string
import bcrypt
from functools import wraps
from datetime import datetime, timezone, timedelta
from flask import Blueprint, jsonify, request
from flask_jwt_extended import jwt_required, get_jwt_identity
from sqlalchemy import func
from db import db
from models import User, AnalysisRun, UserAlert

admin_bp = Blueprint("admin", __name__, url_prefix="/admin/api")


def admin_required(fn):
    @wraps(fn)
    @jwt_required()
    def wrapper(*args, **kwargs):
        user_id = get_jwt_identity()
        user = User.query.get(int(user_id))
        if not user or not user.is_admin:
            return jsonify({"error": "Admin access required."}), 403
        return fn(*args, **kwargs)
    return wrapper


# ── Bootstrap: promote first admin ────────────────────────────────────────────
@admin_bp.route("/setup", methods=["POST"])
def setup_admin():
    if User.query.filter_by(is_admin=True).count() > 0:
        return jsonify({"error": "Admin already configured."}), 403
    data  = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    user  = User.query.filter_by(email=email).first()
    if not user:
        return jsonify({"error": "User not found. Register first."}), 404
    user.is_admin = True
    db.session.commit()
    return jsonify({"message": f"{email} is now admin."})


# ── Stats overview ─────────────────────────────────────────────────────────────
@admin_bp.route("/stats")
@admin_required
def stats():
    total_users    = User.query.count()
    verified_users = User.query.filter_by(email_verified=True).count()
    active_users   = User.query.filter_by(is_active=True).count()
    admin_users    = User.query.filter_by(is_admin=True).count()
    total_runs     = AnalysisRun.query.count()

    today      = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    runs_today = AnalysisRun.query.filter(AnalysisRun.run_at >= today).count()

    latest_run = AnalysisRun.query.order_by(AnalysisRun.run_at.desc()).first()

    sentiment_dist = db.session.query(
        AnalysisRun.sentiment, func.count(AnalysisRun.id)
    ).group_by(AnalysisRun.sentiment).all()

    # New users in last 7 days
    week_ago   = datetime.now(timezone.utc) - timedelta(days=7)
    new_7d     = User.query.filter(User.created_at >= week_ago).count()

    # Sign-up trend: users per day for last 14 days
    signup_trend = []
    for i in range(13, -1, -1):
        day_start = (datetime.now(timezone.utc) - timedelta(days=i)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_end   = day_start + timedelta(days=1)
        count     = User.query.filter(User.created_at >= day_start, User.created_at < day_end).count()
        signup_trend.append({"date": day_start.strftime("%d %b"), "count": count})

    return jsonify({
        "users": {
            "total":    total_users,
            "verified": verified_users,
            "active":   active_users,
            "admins":   admin_users,
            "new_7d":   new_7d,
        },
        "runs": {
            "total":  total_runs,
            "today":  runs_today,
            "latest": latest_run.run_at.strftime("%d %b %Y, %H:%M UTC") if latest_run else None,
        },
        "sentiment_distribution": {s: c for s, c in sentiment_dist},
        "signup_trend": signup_trend,
    })


# ── User management ───────────────────────────────────────────────────────────
@admin_bp.route("/users")
@admin_required
def list_users():
    users = User.query.order_by(User.created_at.desc()).all()
    return jsonify([{
        "id":             u.id,
        "email":          u.email,
        "email_verified": u.email_verified,
        "is_active":      u.is_active,
        "is_admin":       u.is_admin,
        "created_at":     u.created_at.strftime("%d %b %Y") if u.created_at else "",
    } for u in users])


@admin_bp.route("/users/<int:user_id>/activate", methods=["POST"])
@admin_required
def activate_user(user_id):
    user = User.query.get(user_id)
    if not user:
        return jsonify({"error": "User not found."}), 404
    user.is_active = True
    db.session.commit()
    return jsonify({"message": "User activated."})


@admin_bp.route("/users/<int:user_id>/deactivate", methods=["POST"])
@admin_required
def deactivate_user(user_id):
    current_id = int(get_jwt_identity())
    if user_id == current_id:
        return jsonify({"error": "Cannot deactivate yourself."}), 400
    user = User.query.get(user_id)
    if not user:
        return jsonify({"error": "User not found."}), 404
    user.is_active = False
    db.session.commit()
    return jsonify({"message": "User deactivated."})


@admin_bp.route("/users/<int:user_id>/toggle-admin", methods=["POST"])
@admin_required
def toggle_admin(user_id):
    current_id = int(get_jwt_identity())
    if user_id == current_id:
        return jsonify({"error": "Cannot modify your own admin status."}), 400
    user = User.query.get(user_id)
    if not user:
        return jsonify({"error": "User not found."}), 404
    user.is_admin = not user.is_admin
    db.session.commit()
    return jsonify({"message": "Admin status updated.", "is_admin": user.is_admin})


@admin_bp.route("/users/<int:user_id>/reset-password", methods=["POST"])
@admin_required
def admin_reset_password(user_id):
    user = User.query.get(user_id)
    if not user:
        return jsonify({"error": "User not found."}), 404

    # Generate a readable temp password: XXXX-XXXX-XXXX (uppercase + digits, no ambiguous chars)
    chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    groups = ["".join(secrets.choice(chars) for _ in range(4)) for _ in range(3)]
    temp_password = "-".join(groups)

    user.password_hash      = bcrypt.hashpw(temp_password.encode(), bcrypt.gensalt()).decode()
    user.tokens_valid_after = datetime.now(timezone.utc)  # invalidate all existing sessions
    db.session.commit()

    return jsonify({"message": f"Password reset for {user.email}.", "temp_password": temp_password})


# ── Analysis history ──────────────────────────────────────────────────────────
@admin_bp.route("/analysis/history")
@admin_required
def analysis_history():
    days  = min(int(request.args.get("days", 7)), 30)
    since = datetime.now(timezone.utc) - timedelta(days=days)
    rows  = (AnalysisRun.query
             .filter(AnalysisRun.run_at >= since)
             .order_by(AnalysisRun.run_at.desc())
             .limit(300)
             .all())
    return jsonify([{
        "id":            r.id,
        "commodity":     r.commodity,
        "run_at":        r.run_at.strftime("%d %b %Y, %H:%M UTC"),
        "sentiment":     r.sentiment,
        "article_count": r.article_count,
    } for r in rows])

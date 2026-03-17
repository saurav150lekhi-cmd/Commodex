import secrets
from datetime import datetime, timezone, timedelta
from db import db


class User(db.Model):
    __tablename__ = "users"

    id                 = db.Column(db.Integer, primary_key=True)
    email              = db.Column(db.String(255), unique=True, nullable=False)
    password_hash      = db.Column(db.String(255), nullable=False)
    is_active          = db.Column(db.Boolean, default=True)
    is_admin           = db.Column(db.Boolean, default=False)
    email_verified     = db.Column(db.Boolean, default=False)
    verification_token = db.Column(db.String(64), nullable=True)
    created_at         = db.Column(db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class PasswordResetToken(db.Model):
    __tablename__ = "password_reset_tokens"

    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    token      = db.Column(db.String(64), unique=True, nullable=False)
    expires_at = db.Column(db.DateTime(timezone=True), nullable=False)
    used       = db.Column(db.Boolean, default=False)

    @staticmethod
    def generate(user_id):
        token = secrets.token_urlsafe(32)
        expires = datetime.now(timezone.utc) + timedelta(hours=1)
        return PasswordResetToken(user_id=user_id, token=token, expires_at=expires)

    def is_valid(self):
        return not self.used and datetime.now(timezone.utc) < self.expires_at


class UserAlert(db.Model):
    __tablename__ = "user_alerts"

    id        = db.Column(db.Integer, primary_key=True)
    user_id   = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    commodity = db.Column(db.String(50), nullable=False)
    enabled   = db.Column(db.Boolean, default=True)

    __table_args__ = (
        db.UniqueConstraint("user_id", "commodity", name="uq_user_alert"),
    )


class AnalysisRun(db.Model):
    __tablename__ = "analysis_runs"

    id            = db.Column(db.Integer, primary_key=True)
    commodity     = db.Column(db.String(50), nullable=False)
    run_at        = db.Column(db.DateTime(timezone=True), nullable=False)
    data          = db.Column(db.JSON, nullable=False)
    article_count = db.Column(db.Integer)
    sentiment     = db.Column(db.String(20))

    __table_args__ = (
        db.Index("ix_analysis_runs_commodity_run_at", "commodity", run_at.desc()),
    )

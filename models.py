from datetime import datetime, timezone
from db import db


class User(db.Model):
    __tablename__ = "users"

    id            = db.Column(db.Integer, primary_key=True)
    email         = db.Column(db.String(255), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    is_active     = db.Column(db.Boolean, default=True)
    created_at    = db.Column(db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class AnalysisRun(db.Model):
    __tablename__ = "analysis_runs"

    id            = db.Column(db.Integer, primary_key=True)
    commodity     = db.Column(db.String(50), nullable=False)
    run_at        = db.Column(db.DateTime(timezone=True), nullable=False)
    data          = db.Column(db.JSON, nullable=False)   # full commodity dict
    article_count = db.Column(db.Integer)
    sentiment     = db.Column(db.String(20))

    __table_args__ = (
        db.Index("ix_analysis_runs_commodity_run_at", "commodity", run_at.desc()),
    )

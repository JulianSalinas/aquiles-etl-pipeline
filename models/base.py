from typing import List, Optional
from sqlalchemy import BINARY, BigInteger, Boolean, Column, DECIMAL, DateTime, ForeignKeyConstraint, Identity, Index, Integer, PrimaryKeyConstraint, Table, Unicode
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import datetime
import decimal

class Base(DeclarativeBase):
    pass

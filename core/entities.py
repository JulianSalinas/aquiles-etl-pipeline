from typing import List, Optional

from sqlalchemy import BINARY, BigInteger, Boolean, Column, DECIMAL, DateTime, ForeignKeyConstraint, Identity, Index, Integer, PrimaryKeyConstraint, Table, Unicode
from sqlalchemy.dialects.mssql import DATETIME2
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import datetime
import decimal

class Base(DeclarativeBase):
    pass


class ExcelFileRaw(Base):
    __tablename__ = 'ExcelFileRaw'
    __table_args__ = (
        PrimaryKeyConstraint('Id', name='PK_ExcelFileRaw'),
    )

    Id: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    ProductName: Mapped[Optional[str]] = mapped_column(Unicode(collation='SQL_Latin1_General_CP1_CI_AS'))
    ProviderName: Mapped[Optional[str]] = mapped_column(Unicode(collation='SQL_Latin1_General_CP1_CI_AS'))
    LastReviewDt: Mapped[Optional[str]] = mapped_column(Unicode(50, 'SQL_Latin1_General_CP1_CI_AS'))
    ProductPrice: Mapped[Optional[str]] = mapped_column(Unicode(50, 'SQL_Latin1_General_CP1_CI_AS'))


class FileStatus(Base):
    __tablename__ = 'FileStatus'
    __table_args__ = (
        PrimaryKeyConstraint('Id', name='PK_FileStatus'),
        Index('UQ_FileStatus_StatusName', 'StatusName', unique=True)
    )

    Id: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    StatusName: Mapped[str] = mapped_column(Unicode(50, 'SQL_Latin1_General_CP1_CI_AS'))

    ProcessFile: Mapped[List['ProcessFile']] = relationship('ProcessFile', back_populates='FileStatus_')


class Provider(Base):
    __tablename__ = 'Provider'
    __table_args__ = (
        PrimaryKeyConstraint('Id', name='PK_Provider'),
    )

    Id: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    Name: Mapped[str] = mapped_column(Unicode(255, 'SQL_Latin1_General_CP1_CI_AS'))
    CreateDt: Mapped[datetime.datetime] = mapped_column(DateTime)

    ProviderSynonym: Mapped[List['ProviderSynonym']] = relationship('ProviderSynonym', back_populates='Provider_')
    Provider_Product: Mapped[List['ProviderProduct']] = relationship('ProviderProduct', back_populates='Provider_')


class UnitOfMeasure(Base):
    __tablename__ = 'UnitOfMeasure'
    __table_args__ = (
        PrimaryKeyConstraint('Id', name='PK_UnitOfMeasure_Id'),
    )

    Id: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    Acronym: Mapped[str] = mapped_column(Unicode(10, 'SQL_Latin1_General_CP1_CI_AS'))
    Name: Mapped[str] = mapped_column(Unicode(50, 'SQL_Latin1_General_CP1_CI_AS'))

    Product: Mapped[List['Product']] = relationship('Product', back_populates='UnitOfMeasure_')
    UnitOfMeasureAcronym: Mapped[List['UnitOfMeasureAcronym']] = relationship('UnitOfMeasureAcronym', back_populates='UnitOfMeasure_')


t_systranschemas = Table(
    'systranschemas', Base.metadata,
    Column('tabid', Integer, nullable=False),
    Column('startlsn', BINARY(10), nullable=False),
    Column('endlsn', BINARY(10), nullable=False),
    Column('typeid', Integer, nullable=False),
    Index('uncsystranschemas', 'startlsn', unique=True)
)


class ProcessFile(Base):
    __tablename__ = 'ProcessFile'
    __table_args__ = (
        ForeignKeyConstraint(['StatusId'], ['FileStatus.Id'], name='FK_ProcessFile_FileStatus'),
        PrimaryKeyConstraint('Id', name='PK_ProcessFile'),
        Index('IX_ProcessFile_FileName', 'FileName')
    )

    Id: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    Container: Mapped[str] = mapped_column(Unicode(256, 'SQL_Latin1_General_CP1_CI_AS'))
    FileName: Mapped[str] = mapped_column(Unicode(512, 'SQL_Latin1_General_CP1_CI_AS'))
    StatusId: Mapped[int] = mapped_column(Integer)
    ProcessDt: Mapped[datetime.datetime] = mapped_column(DATETIME2)
    BlobSize: Mapped[Optional[int]] = mapped_column(BigInteger)
    ContentType: Mapped[Optional[str]] = mapped_column(Unicode(128, 'SQL_Latin1_General_CP1_CI_AS'))
    CreatedDt: Mapped[Optional[datetime.datetime]] = mapped_column(DATETIME2)
    LastModifiedDt: Mapped[Optional[datetime.datetime]] = mapped_column(DATETIME2)
    ETag: Mapped[Optional[str]] = mapped_column(Unicode(128, 'SQL_Latin1_General_CP1_CI_AS'))
    Metadata: Mapped[Optional[str]] = mapped_column(Unicode(collation='SQL_Latin1_General_CP1_CI_AS'))

    FileStatus_: Mapped['FileStatus'] = relationship('FileStatus', back_populates='ProcessFile')


class Product(Base):
    __tablename__ = 'Product'
    __table_args__ = (
        ForeignKeyConstraint(['UnitOfMeasureId'], ['UnitOfMeasure.Id'], name='FK_Product_UnitOfMeasure'),
        PrimaryKeyConstraint('Id', name='PK_Product')
    )

    Id: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    Price: Mapped[decimal.Decimal] = mapped_column(DECIMAL(18, 2))
    Description: Mapped[Optional[str]] = mapped_column(Unicode(collation='SQL_Latin1_General_CP1_CI_AS'))
    Measure: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(18, 2))
    UnitOfMeasureId: Mapped[Optional[int]] = mapped_column(Integer)
    CreatedDt: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)
    UpdatedDt: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime)

    UnitOfMeasure_: Mapped[Optional['UnitOfMeasure']] = relationship('UnitOfMeasure', back_populates='Product')
    Provider_Product: Mapped[List['ProviderProduct']] = relationship('ProviderProduct', back_populates='Product_')


class ProviderSynonym(Base):
    __tablename__ = 'ProviderSynonym'
    __table_args__ = (
        ForeignKeyConstraint(['ProviderId'], ['Provider.Id'], name='FK_ProviderSynonym_Provider'),
        PrimaryKeyConstraint('Id', name='PK_ProviderSynonym')
    )

    Id: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    Synonym: Mapped[str] = mapped_column(Unicode(255, 'SQL_Latin1_General_CP1_CI_AS'))
    ProviderId: Mapped[int] = mapped_column(Integer)

    Provider_: Mapped['Provider'] = relationship('Provider', back_populates='ProviderSynonym')


class UnitOfMeasureAcronym(Base):
    __tablename__ = 'UnitOfMeasureAcronym'
    __table_args__ = (
        ForeignKeyConstraint(['UnitOfMeasureId'], ['UnitOfMeasure.Id'], name='FK_UnitOfMeasureAcronym_UnitOfMeasure'),
        PrimaryKeyConstraint('Id', name='PK_UnitOfMeasureAcronym')
    )

    Id: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    Acronym: Mapped[str] = mapped_column(Unicode(50, 'SQL_Latin1_General_CP1_CI_AS'))
    UnitOfMeasureId: Mapped[int] = mapped_column(Integer)

    UnitOfMeasure_: Mapped['UnitOfMeasure'] = relationship('UnitOfMeasure', back_populates='UnitOfMeasureAcronym')


class ProviderProduct(Base):
    __tablename__ = 'Provider_Product'
    __table_args__ = (
        ForeignKeyConstraint(['ProductId'], ['Product.Id'], name='FK_Provider_Product_Product'),
        ForeignKeyConstraint(['ProviderId'], ['Provider.Id'], name='FK_Provider_Product_Provider'),
        PrimaryKeyConstraint('Id', name='PK_Provider_Product')
    )

    Id: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    ProductId: Mapped[int] = mapped_column(Integer)
    ProviderId: Mapped[int] = mapped_column(Integer)
    IsValidated: Mapped[bool] = mapped_column(Boolean)
    LastReviewDt: Mapped[Optional[datetime.datetime]] = mapped_column(DATETIME2)
    PackageUnits: Mapped[Optional[int]] = mapped_column(Integer)
    IVA: Mapped[Optional[decimal.Decimal]] = mapped_column(DECIMAL(18, 2))

    Product_: Mapped['Product'] = relationship('Product', back_populates='Provider_Product')
    Provider_: Mapped['Provider'] = relationship('Provider', back_populates='Provider_Product')

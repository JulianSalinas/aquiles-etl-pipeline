from .base import *

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

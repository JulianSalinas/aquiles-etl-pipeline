from .base import *

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

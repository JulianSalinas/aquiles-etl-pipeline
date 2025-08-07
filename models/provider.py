from .base import *

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

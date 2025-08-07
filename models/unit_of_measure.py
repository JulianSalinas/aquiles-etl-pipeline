from .base import *

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

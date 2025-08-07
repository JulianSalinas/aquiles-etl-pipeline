from .base import *

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

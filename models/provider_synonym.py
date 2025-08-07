from .base import *

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

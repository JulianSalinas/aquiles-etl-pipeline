from .base import *

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

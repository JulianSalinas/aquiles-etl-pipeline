from .base import *

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

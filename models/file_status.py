from .base import *

class FileStatus(Base):
    __tablename__ = 'FileStatus'
    __table_args__ = (
        PrimaryKeyConstraint('Id', name='PK_FileStatus'),
        Index('UQ_FileStatus_StatusName', 'StatusName', unique=True)
    )

    Id: Mapped[int] = mapped_column(Integer, Identity(start=1, increment=1), primary_key=True)
    StatusName: Mapped[str] = mapped_column(Unicode(50, 'SQL_Latin1_General_CP1_CI_AS'))

    ProcessFile: Mapped[List['ProcessFile']] = relationship('ProcessFile', back_populates='FileStatus_')

--DROP DATABASE Kafka
CREATE DATABASE Kafka
GO

USE Kafka;
GO

CREATE TABLE dbo.Login
(
    [LoginId]    BIGINT       NOT NULL IDENTITY,
    [Email]      VARCHAR(100) NOT NULL,
    [Time]       DATETIME     NOT NULL,
    [SessionId]  VARCHAR(50)  NOT NULL,
    [InsertDate] DATETIME2    NOT NULL,
    [UpdateDate] DATETIME2    NULL,
    CONSTRAINT PK_Login PRIMARY KEY ([LoginId])
);
GO

CREATE TABLE dbo.LoginMessage
(
    [Partition]  INT          NOT NULL,
    [Offset]     BIGINT       NOT NULL,
    [Message]    VARCHAR(MAX) NOT NULL,
    [Sha5]       BINARY(20)   NOT NULL,
    [InsertDate] DATETIME2    NOT NULL
        CONSTRAINT PK_LoginMessage PRIMARY KEY ([Partition], [Offset], [Sha5])
);
GO

CREATE TABLE dbo.Logout
(
    [LogoutId]   BIGINT      NOT NULL IDENTITY,
    [Time]       DATETIME    NOT NULL,
    [SessionId]  VARCHAR(50) NOT NULL,
    [InsertDate] DATETIME2   NOT NULL,
    [UpdateDate] DATETIME2   NULL
        CONSTRAINT PK_Logout PRIMARY KEY ([LogoutId])
);
GO

CREATE TABLE dbo.LogoutMessage
(
    [Partition]  INT          NOT NULL,
    [Offset]     BIGINT       NOT NULL,
    [Message]    VARCHAR(MAX) NOT NULL,
    [Sha5]       BINARY(20)   NOT NULL,
    [InsertDate] DATETIME2    NOT NULL,
    CONSTRAINT PK_LogoutMessage PRIMARY KEY ([Partition], [Offset], [Sha5])
);
GO

CREATE TABLE dbo.Error
(
    [ErrorId]   BIGINT       NOT NULL IDENTITY,
    [Topic]     VARCHAR(200) NOT NULL,
    [Partition] INT          NOT NULL,
    [Offset]    BIGINT       NOT NULL,
    [Message]   VARCHAR(MAX) NOT NULL,
    [Sha5]      BINARY(20)   NOT NULL,
    [Error]     VARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_Error PRIMARY KEY ([ErrorId])
);
GO

CREATE PROCEDURE dbo.InsertLogin
AS
BEGIN
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;
    BEGIN TRY
        INSERT INTO dbo.Error
            ([Topic], [Partition], [Offset], [Message], [Sha5], [Error])
        SELECT [Topic], [Partition], [Offset], [Message], [Sha5], [Error]
        FROM #Error;

        MERGE dbo.Login AS Target
        USING #Login AS Source
        ON Source.SessionId = Target.SessionId
        WHEN NOT MATCHED THEN
            INSERT ([Email], [Time], [SessionId], [InsertDate])
            VALUES ([Email], [Time], [SessionId], GETDATE())
        WHEN MATCHED AND Target.[Time] < Source.[Time]
            THEN
            UPDATE SET [Time] = Source.[Time];

        MERGE dbo.LoginMessage AS Target
        USING #KafkaMessage AS Source
        ON Source.[Partition] = Target.[Partition]
            AND Source.[Offset] = Target.[Offset]
            AND Source.[Sha5] = Target.[Sha5]
        WHEN NOT MATCHED THEN
            INSERT ([Partition], [Offset], [Sha5], [Message], [InsertDate])
            VALUES ([Partition], [Offset], [Sha5], [Message], GETDATE());

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END
GO

CREATE PROCEDURE dbo.InsertLogout
AS
BEGIN
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;
    BEGIN TRY
        INSERT INTO dbo.Error
            ([Topic], [Partition], [Offset], [Message], [Sha5], [Error])
        SELECT [Topic], [Partition], [Offset], [Message], [Sha5], [Error]
        FROM #Error;

        MERGE dbo.Logout AS Target
        USING #Logout AS Source
        ON Target.[SessionId] = Source.[SessionId]
        WHEN NOT MATCHED THEN
            INSERT ([Time], [SessionId], [InsertDate])
            VALUES ([Time], [SessionId], GETDATE())
        WHEN MATCHED AND Target.[Time] < Source.[Time]
            THEN
            UPDATE
            SET [Time]       = Source.[Time],
                [UpdateDate] = GETDATE();

        MERGE dbo.LogoutMessage AS Target
        USING #KafkaMessage AS Source
        ON Source.[Partition] = Target.[Partition]
            AND Source.[Offset] = Target.[Offset]
            AND Source.[Sha5] = Target.[Sha5]
        WHEN NOT MATCHED THEN
            INSERT ([Partition], [Offset], [Sha5], [Message], [InsertDate])
            VALUES ([Partition], [Offset], [Sha5], [Message], GETDATE());

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END
GO


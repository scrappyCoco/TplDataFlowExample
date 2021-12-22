using Coding4fun.Tpl.DataFlow.Shared;

namespace KafkaConsumer.Db;

public class MsLogoutBatchHandler: MsSqlBatchHandler<LogoutMessage>
{
    /// <inheritdoc />
    public MsLogoutBatchHandler(string connectionString) : base(connectionString)
    {
    }

    /// <inheritdoc />
    protected override string TempTableDeclaration => @"
CREATE TABLE #Logout (
    Time      DATETIME,
    SessionId VARCHAR(50)
);";

    /// <inheritdoc />
    protected override string TempTableName => "#Logout";

    /// <inheritdoc />
    protected override void AddEntityInternal(LogoutMessage entity)
    {
        DataTable.Rows.Add(entity.Time, entity.SessionId);
    }
}
using Coding4fun.Tpl.DataFlow.Shared;

namespace KafkaConsumer.Db;

public class MsLoginBatchHandler: MsSqlBatchHandler<LoginMessage>
{
    /// <inheritdoc />
    public MsLoginBatchHandler(string connectionString) : base(connectionString)
    {
    }

    /// <inheritdoc />
    protected override string TempTableDeclaration => @"
CREATE TABLE #Login (
    Email     VARCHAR(100),
    Time      DATETIME,
    SessionId VARCHAR(50)
);";

    /// <inheritdoc />
    protected override string TempTableName => "#Login";

    /// <inheritdoc />
    protected override void AddEntityInternal(LoginMessage entity)
    {
        DataTable.Rows.Add(entity.Email, entity.Time, entity.SessionId);
    }
}
using System.Data;
using Coding4fun.Tpl.DataFlow.Shared;
using KafkaConsumer.Config;

namespace KafkaConsumer.Db;

public class MsLoginBatchHandler : MsSqlBatchHandler
{
    private readonly DataTable _logoutDt;

    /// <inheritdoc />
    public MsLoginBatchHandler(MsSqlConfig config) : base(config, CreateDataTables()) => _logoutDt = DataTables[0];

    /// <inheritdoc />
    protected override string TempTableDeclaration => @"
CREATE TABLE #Login (
    Email     VARCHAR(100),
    Time      DATETIME,
    SessionId VARCHAR(50)
);";

    /// <inheritdoc />
    protected override string ProcedureName => "dbo.InsertLogin";

    private static DataTable[] CreateDataTables()
    {
        DataTable dataTable = new DataTable("#Login");
        dataTable.Columns.Add(nameof(LoginMessage.Email), typeof(string));
        dataTable.Columns.Add(nameof(LoginMessage.Time), typeof(DateTime));
        dataTable.Columns.Add(nameof(LoginMessage.SessionId), typeof(string));
        return new[] { dataTable };
    }

    /// <inheritdoc />
    protected override void AddEntityInternal(object entity)
    {
        var login = (LoginMessage)entity;
        _logoutDt.Rows.Add(login.Email, login.Time, login.SessionId);
    }
}
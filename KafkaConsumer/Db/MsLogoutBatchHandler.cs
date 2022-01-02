using System.Data;
using Coding4fun.Tpl.DataFlow.Shared;
using KafkaConsumer.Config;

namespace KafkaConsumer.Db;

public class MsLogoutBatchHandler : MsSqlBatchHandler
{
    private readonly DataTable _loginDt;

    /// <inheritdoc />
    public MsLogoutBatchHandler(MsSqlConfig config) : base(config, CreateDataTables()) => _loginDt = DataTables[0];

    /// <inheritdoc />
    protected override string TempTableDeclaration => @"
CREATE TABLE #Logout (
    Time      DATETIME,
    SessionId VARCHAR(50)
);";

    /// <inheritdoc />
    protected override string ProcedureName => "dbo.InsertLogout";

    private static DataTable[] CreateDataTables()
    {
        DataTable dataTable = new DataTable("#Logout");
        dataTable.Columns.Add(nameof(LogoutMessage.Time), typeof(DateTime));
        dataTable.Columns.Add(nameof(LogoutMessage.SessionId), typeof(string));
        return new[] { dataTable };
    }

    /// <inheritdoc />
    protected override void AddEntityInternal(object entity)
    {
        var logout = (LogoutMessage)entity;
        _loginDt.Rows.Add(logout.Time, logout.SessionId);
    }
}
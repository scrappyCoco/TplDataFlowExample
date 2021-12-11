using System.Data;
using System.Data.SqlClient;

namespace KafkaConsumer;

public static class MsSqlConsumer
{
    public static async Task InsertLoginAsync(string connectionString, DataTable dataTable)
    {
        const string createSignInTable = @"
CREATE TABLE #Login (
    Email     VARCHAR(100),
    Time      DATETIME,
    SessionId VARCHAR(50)
);"; 

        await InsertAsync(connectionString, dataTable, createSignInTable, "#Login");
    }

    public static async Task InsertLogoutAsync(string connectionString, DataTable dataTable)
    {
        const string crateSignOutTable = @"
CREATE TABLE #Logout (
    Time      DATETIME,
    SessionId VARCHAR(50)
);"; 
        
        await InsertAsync(connectionString, dataTable, crateSignOutTable, "#Logout");
    }

    private static async Task InsertAsync(string connectionString, DataTable dataTable, string tempTableDeclaration,
        string tempTableName)
    {

        await using SqlConnection connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        await using SqlCommand createTableCommand = new SqlCommand(tempTableDeclaration, connection);
        createTableCommand.ExecuteNonQuery();

        using SqlBulkCopy sqlBulkCopy = new(connection);
        sqlBulkCopy.DestinationTableName = tempTableName;
        await sqlBulkCopy.WriteToServerAsync(dataTable);

        dataTable.Clear();
    }
}
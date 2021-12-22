using System.Data;
using System.Data.SqlClient;

namespace KafkaConsumer.Db;

public abstract class MsSqlBatchHandler<TEntity>
{
    private readonly string _connectionString;
    private const int DefaultBatchSize = 10_000;
    private int _batchSize = DefaultBatchSize;
    private readonly SemaphoreSlim _semaphoreSlim = new(1, 1);

    protected MsSqlBatchHandler(string connectionString) => _connectionString = connectionString;

    public int BatchSize
    {
        get => _batchSize;
        set
        {
            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(BatchSize), $"{nameof(BatchSize)} must be >= 0");
            }

            _batchSize = value;
        }
    }

    protected abstract string TempTableDeclaration { get; }

    protected abstract string TempTableName { get; }

    public async Task AddEntityAsync(TEntity entity)
    {
        await _semaphoreSlim.WaitAsync();
        try
        {
            AddEntityInternal(entity);
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }

    protected DataTable DataTable { get; } = new();

    protected abstract void AddEntityInternal(TEntity entity);

    public bool IsBatchFull => DataTable.Rows.Count >= BatchSize;
    
    public async Task FlushBatchAsync()
    {
        await _semaphoreSlim.WaitAsync();
        try
        {

            await using SqlConnection connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            await using SqlCommand createTableCommand = new SqlCommand(TempTableDeclaration, connection);
            createTableCommand.ExecuteNonQuery();

            using SqlBulkCopy sqlBulkCopy = new(connection);
            sqlBulkCopy.DestinationTableName = TempTableName;
            await sqlBulkCopy.WriteToServerAsync(DataTable);

            DataTable.Clear();
        }
        finally
        {
            _semaphoreSlim.Release();
        }
    }
}
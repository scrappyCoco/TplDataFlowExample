using System.Data;
using System.Data.SqlClient;
using Coding4fun.Tpl.DataFlow.Shared;
using KafkaConsumer.Config;

namespace KafkaConsumer.Db;

public abstract class MsSqlBatchHandler
{
    private const int DefaultBatchSize = 1_000;

    private const string CommonDefinition = @"
CREATE TABLE #Error  (
    [Topic]     VARCHAR(200) NOT NULL,
    [Partition] INT         NOT NULL,
    [Offset]    BIGINT       NOT NULL,
    [Message]   VARCHAR(MAX) NOT NULL,
    [Sha5]      BINARY(20)   NOT NULL,
    [Error]     VARCHAR(MAX) NOT NULL
);

CREATE TABLE #KafkaMessage (
    [Partition]  INT          NOT NULL,
    [Offset]     BIGINT       NOT NULL,
    [Message]    VARCHAR(MAX) NOT NULL,
    [Sha5]       BINARY(20)   NOT NULL
);
";

    private readonly string _connectionString;
    private readonly SemaphoreSlim _dataTableSemaphoreSlim = new(1, 1);
    private readonly DataTable _errorsDataTable;
    private readonly DataTable _kafkaMessageDataTable;
    private int _batchSize;
    private int _insertedCount;

    protected MsSqlBatchHandler(MsSqlConfig config, DataTable[] dataTables)
    {
        _connectionString = config.ConnectionString;
        _batchSize = config.BatchSize ?? DefaultBatchSize;
        DataTables = dataTables;
        _insertedCount = 0;

        _errorsDataTable = new DataTable("#Error");
        _errorsDataTable.Columns.Add("Topic", typeof(string));
        _errorsDataTable.Columns.Add("Partition", typeof(int));
        _errorsDataTable.Columns.Add("Offset", typeof(long));
        _errorsDataTable.Columns.Add("Message", typeof(string));
        _errorsDataTable.Columns.Add("Error", typeof(string));

        _kafkaMessageDataTable = new DataTable("#KafkaMessage");
        _kafkaMessageDataTable.Columns.Add("Partition", typeof(int));
        _kafkaMessageDataTable.Columns.Add("Offset", typeof(long));
        _kafkaMessageDataTable.Columns.Add("Message", typeof(string));
        _kafkaMessageDataTable.Columns.Add("Sha5", typeof(byte[]));
    }

    public int BatchSize
    {
        get => _batchSize;
        set => _batchSize = value;
    }

    protected abstract string TempTableDeclaration { get; }

    protected abstract string ProcedureName { get; }

    protected DataTable[] DataTables { get; }

    public bool IsBatchFull => _insertedCount >= BatchSize;

    public async Task AddEntityAsync(KafkaEntity kafkaEntity)
    {
        await _dataTableSemaphoreSlim.WaitAsync();
        try
        {
            ++_insertedCount;
            if (kafkaEntity.Item == null)
            {
                _errorsDataTable.Rows.Add(
                    kafkaEntity.KafkaMessage.Topic,
                    kafkaEntity.KafkaMessage.Partition.Value,
                    kafkaEntity.KafkaMessage.Offset.Value,
                    kafkaEntity.KafkaMessage.Message,
                    kafkaEntity.Error
                );
            }
            else
            {
                _kafkaMessageDataTable.Rows.Add(
                    kafkaEntity.KafkaMessage.Partition.Value,
                    kafkaEntity.KafkaMessage.Offset.Value,
                    kafkaEntity.KafkaMessage.Message,
                    kafkaEntity.MessageHash
                );

                AddEntityInternal(kafkaEntity.Item);
            }
        }
        finally
        {
            _dataTableSemaphoreSlim.Release();
        }
    }

    protected abstract void AddEntityInternal(object entity);

    public async Task<int> FlushBatchAsync()
    {
        await _dataTableSemaphoreSlim.WaitAsync();
        try
        {
            if (_insertedCount == 0) return 0;

            await using SqlConnection connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            string tempTableDefinitions = CommonDefinition + TempTableDeclaration;
            await using SqlCommand createTableCommand = new SqlCommand(tempTableDefinitions, connection);
            createTableCommand.ExecuteNonQuery();

            var dataTables = DataTables.Concat(new[] { _errorsDataTable, _kafkaMessageDataTable });
            foreach (DataTable dataTable in dataTables)
            {
                using SqlBulkCopy sqlBulkCopy = new(connection);
                sqlBulkCopy.DestinationTableName = dataTable.TableName;
                await sqlBulkCopy.WriteToServerAsync(dataTable);
                dataTable.Clear();
            }

            await using SqlCommand procedureCommand = new SqlCommand(ProcedureName, connection)
            {
                CommandType = CommandType.StoredProcedure
            };
            await procedureCommand.ExecuteNonQueryAsync();

            return _insertedCount;
        }
        finally
        {
            _insertedCount = 0;
            _dataTableSemaphoreSlim.Release();
        }
    }
}
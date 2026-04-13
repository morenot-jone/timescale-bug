using Dapper;
using Npgsql;
using Testcontainers.PostgreSql;

namespace TimescaleBug.Tests;

/// <summary>
/// Shared base for integration tests that need a TimescaleDB container
/// with the schema created from the repository migration files.
/// </summary>
public abstract class TimescaleTestBase
{
    private PostgreSqlContainer? _postgres;
    private string? _connectionString;

    protected string ConnectionString =>
        _connectionString ?? throw new InvalidOperationException("Container not started. Call StartContainerAsync first.");

    protected virtual string TimescaleDbImage => "timescale/timescaledb:2.24.0-pg18";

    protected async Task StartContainerAsync()
    {
        _postgres = new PostgreSqlBuilder()
            .WithImage(TimescaleDbImage)
            .WithDatabase("db")
            .WithUsername("testuser")
            .WithPassword("testpass")
            .Build();

        await _postgres.StartAsync();
        _connectionString = _postgres.GetConnectionString();
    }

    protected async Task RunMigrationsAsync()
    {
        await using NpgsqlConnection conn = new(ConnectionString);
        await conn.OpenAsync();

        string solutionRoot = GetSolutionRoot();
        string databasePath = Path.Combine(solutionRoot, "database");

        await ExecuteSqlFileAsync(conn, Path.Combine(databasePath, "00-init-schema.sql"));
        await ExecuteSqlFileAsync(conn, Path.Combine(databasePath, "01-migrations.sql"));
        await ExecuteSqlFileAsync(conn, Path.Combine(databasePath, "02-create-optimized-measurements.sql"));
    }

    protected async Task StopContainerAsync()
    {
        if (_postgres != null)
        {
            await _postgres.DisposeAsync();
            _postgres = null;
            _connectionString = null;
        }
    }

    protected async Task TruncateMeasurementsAsync()
    {
        await using NpgsqlConnection conn = new(ConnectionString);
        await conn.OpenAsync();
        await conn.ExecuteAsync("TRUNCATE TABLE measurements;");
    }

    private static string GetSolutionRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory != null && !File.Exists(Path.Combine(directory.FullName, "TimescaleBug.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName
            ?? throw new InvalidOperationException("Could not find solution root directory");
    }

    private static async Task ExecuteSqlFileAsync(NpgsqlConnection connection, string filePath)
    {
        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException($"SQL file not found: {filePath}");
        }

        string sqlScript = await File.ReadAllTextAsync(filePath);
        await using NpgsqlCommand command = new(sqlScript, connection);
        await command.ExecuteNonQueryAsync();
    }
}

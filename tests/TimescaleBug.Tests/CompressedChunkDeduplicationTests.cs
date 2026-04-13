using Dapper;
using Npgsql;

namespace TimescaleBug.Tests;

/// <summary>
/// Asserts correct INSERT ... ON CONFLICT DO NOTHING behaviour against
/// compressed hypertable chunks.
///
/// All four tests expect zero duplicates after re-inserting the same rows.
/// The NULL-nullable-col tests FAIL because of a TimescaleDB bug:
/// compressed chunks cannot match on NULL columns in the unique constraint
/// (SQL NULL != NULL), so duplicates slip through.
/// </summary>
[TestFixture]
public class CompressedChunkDeduplicationTests : TimescaleTestBase
{
    [SetUp]
    public async Task SetUp()
    {
        await StartContainerAsync();
        await RunMigrationsAsync();
    }

    [TearDown]
    public async Task TearDown()
    {
        await StopContainerAsync();
    }

    [Test]
    public async Task OnConflictDoNothing_ShouldPreventDuplicates_InCompressedChunks_WithNullableColNull()
    {
        await using NpgsqlConnection conn = new(ConnectionString);
        await conn.OpenAsync();

        // Insert multiple rows into a chunk that will be compressed.
        await conn.ExecuteAsync(@"
            INSERT INTO measurements
                (time, col_a, col_b, col_c, col_d, col_e, col_f, nullable_col)
            VALUES
                ('2025-01-01 12:00:00+00', 'aaa', 'bbb', 1, 22.5, 'eee', 'fff', 'ggg'),
                ('2025-01-01 12:00:00+00', 'aaa', 'zzz', 1, 51.7, 'qqq', 'fff', NULL),
                ('2025-01-01 12:00:00+00', 'aaa', 'zzz', 1, -10.1, 'rrr', 'fff', NULL);
        ");

        int rowsBefore = await conn.ExecuteScalarAsync<int>(
            "SELECT COUNT(*) FROM measurements;");
        Assert.That(rowsBefore, Is.EqualTo(3));

        // Compress the chunk
        await conn.ExecuteAsync(@"
            SELECT compress_chunk(c, if_not_compressed => true)
            FROM show_chunks('measurements') c;
        ");

        int compressedChunks = await conn.ExecuteScalarAsync<int>(@"
            SELECT COUNT(*) FROM timescaledb_information.chunks
            WHERE hypertable_name = 'measurements' AND is_compressed = true;
        ");
        Assert.That(compressedChunks, Is.GreaterThan(0), "Chunk should be compressed");

        // Re-insert the exact same rows using a multi-row VALUES + ON CONFLICT DO NOTHING.
        await conn.ExecuteAsync(@"
            INSERT INTO measurements
                (time, col_a, col_b, col_c, col_d, col_e, col_f, nullable_col)
            VALUES
                ('2025-01-01 12:00:00+00', 'aaa', 'bbb', 1, 22.5, 'eee', 'fff', 'ggg'),
                ('2025-01-01 12:00:00+00', 'aaa', 'zzz', 1, 51.7, 'qqq', 'fff', NULL),
                ('2025-01-01 12:00:00+00', 'aaa', 'zzz', 1, -10.1, 'rrr', 'fff', NULL)
            ON CONFLICT DO NOTHING;
        ");

        int rowsAfter = await conn.ExecuteScalarAsync<int>(
            "SELECT COUNT(*) FROM measurements;");

        // BUG: This fails because TimescaleDB compressed chunks cannot match NULL
        // columns in the unique constraint, so duplicates are created.
        Assert.That(rowsAfter, Is.EqualTo(3),
            "ON CONFLICT DO NOTHING should prevent duplicates in compressed chunks");
    }

    [Test]
    public async Task OnConflictDoNothing_ShouldPreventDuplicates_InCompressedChunks_WithNullableColNull_SingleRowInserts()
    {
        await using NpgsqlConnection conn = new(ConnectionString);
        await conn.OpenAsync();

        // Insert rows one at a time into a chunk that will be compressed
        await conn.ExecuteAsync(@"
            INSERT INTO measurements
                (time, col_a, col_b, col_c, col_d, col_e, col_f, nullable_col)
            VALUES
                ('2025-01-01 12:00:00+00', 'aaa', 'bbb', 1, 22.5, 'eee', 'fff', 'ggg');
        ");
        await conn.ExecuteAsync(@"
            INSERT INTO measurements
                (time, col_a, col_b, col_c, col_d, col_e, col_f, nullable_col)
            VALUES
                ('2025-01-01 12:00:00+00', 'aaa', 'zzz', 1, 51.7, 'qqq', 'fff', NULL);
        ");
        await conn.ExecuteAsync(@"
            INSERT INTO measurements
                (time, col_a, col_b, col_c, col_d, col_e, col_f, nullable_col)
            VALUES
                ('2025-01-01 12:00:00+00', 'aaa', 'zzz', 1, -10.1, 'rrr', 'fff', NULL);
        ");

        int rowsBefore = await conn.ExecuteScalarAsync<int>(
            "SELECT COUNT(*) FROM measurements;");
        Assert.That(rowsBefore, Is.EqualTo(3));

        // Compress the chunk
        await conn.ExecuteAsync(@"
            SELECT compress_chunk(c, if_not_compressed => true)
            FROM show_chunks('measurements') c;
        ");

        int compressedChunks = await conn.ExecuteScalarAsync<int>(@"
            SELECT COUNT(*) FROM timescaledb_information.chunks
            WHERE hypertable_name = 'measurements' AND is_compressed = true;
        ");
        Assert.That(compressedChunks, Is.GreaterThan(0), "Chunk should be compressed");

        // Re-insert the exact same rows one at a time with ON CONFLICT DO NOTHING
        await conn.ExecuteAsync(@"
            INSERT INTO measurements
                (time, col_a, col_b, col_c, col_d, col_e, col_f, nullable_col)
            VALUES
                ('2025-01-01 12:00:00+00', 'aaa', 'bbb', 1, 22.5, 'eee', 'fff', 'ggg')
            ON CONFLICT DO NOTHING;
        ");
        await conn.ExecuteAsync(@"
            INSERT INTO measurements
                (time, col_a, col_b, col_c, col_d, col_e, col_f, nullable_col)
            VALUES
                ('2025-01-01 12:00:00+00', 'aaa', 'zzz', 1, 51.7, 'qqq', 'fff', NULL)
            ON CONFLICT DO NOTHING;
        ");
        await conn.ExecuteAsync(@"
            INSERT INTO measurements
                (time, col_a, col_b, col_c, col_d, col_e, col_f, nullable_col)
            VALUES
                ('2025-01-01 12:00:00+00', 'aaa', 'zzz', 1, -10.1, 'rrr', 'fff', NULL)
            ON CONFLICT DO NOTHING;
        ");

        int rowsAfter = await conn.ExecuteScalarAsync<int>(
            "SELECT COUNT(*) FROM measurements;");

        // BUG: This fails because TimescaleDB compressed chunks cannot match NULL
        // columns in the unique constraint, so duplicates are created.
        Assert.That(rowsAfter, Is.EqualTo(3),
            "ON CONFLICT DO NOTHING should prevent duplicates with single-row inserts");
    }

    [Test]
    public async Task OnConflictDoNothing_ShouldPreventDuplicates_InCompressedChunks_WithNullableColNotNull()
    {
        await using NpgsqlConnection conn = new(ConnectionString);
        await conn.OpenAsync();

        // Insert multiple rows into a chunk that will be compressed.
        await conn.ExecuteAsync(@"
            INSERT INTO measurements
                (time, col_a, col_b, col_c, col_d, col_e, col_f, nullable_col)
            VALUES
                ('2025-01-01 12:00:00+00', 'aaa', 'bbb', 1, 22.5, 'eee', 'fff', 'ggg'),
                ('2025-01-01 12:00:00+00', 'aaa', 'zzz', 1, 51.7, 'qqq', 'fff', 'hhh'),
                ('2025-01-01 12:00:00+00', 'aaa', 'zzz', 1, -10.1, 'rrr', 'fff', 'hhh');
        ");

        int rowsBefore = await conn.ExecuteScalarAsync<int>(
            "SELECT COUNT(*) FROM measurements;");
        Assert.That(rowsBefore, Is.EqualTo(3));

        // Compress the chunk
        await conn.ExecuteAsync(@"
            SELECT compress_chunk(c, if_not_compressed => true)
            FROM show_chunks('measurements') c;
        ");

        int compressedChunks = await conn.ExecuteScalarAsync<int>(@"
            SELECT COUNT(*) FROM timescaledb_information.chunks
            WHERE hypertable_name = 'measurements' AND is_compressed = true;
        ");
        Assert.That(compressedChunks, Is.GreaterThan(0), "Chunk should be compressed");

        // Re-insert the exact same rows using a multi-row VALUES + ON CONFLICT DO NOTHING.
        await conn.ExecuteAsync(@"
            INSERT INTO measurements
                (time, col_a, col_b, col_c, col_d, col_e, col_f, nullable_col)
            VALUES
                ('2025-01-01 12:00:00+00', 'aaa', 'bbb', 1, 22.5, 'eee', 'fff', 'ggg'),
                ('2025-01-01 12:00:00+00', 'aaa', 'zzz', 1, 51.7, 'qqq', 'fff', 'hhh'),
                ('2025-01-01 12:00:00+00', 'aaa', 'zzz', 1, -10.1, 'rrr', 'fff', 'hhh')
            ON CONFLICT DO NOTHING;
        ");

        int rowsAfter = await conn.ExecuteScalarAsync<int>(
            "SELECT COUNT(*) FROM measurements;");

        // This passes — ON CONFLICT DO NOTHING works correctly when nullable_col is NOT NULL.
        Assert.That(rowsAfter, Is.EqualTo(3),
            "ON CONFLICT DO NOTHING should prevent duplicates in compressed chunks");
    }

    [Test]
    public async Task OnConflictDoNothing_ShouldPreventDuplicates_InCompressedChunks_WithNullableColNotNull_SingleRowInserts()
    {
        await using NpgsqlConnection conn = new(ConnectionString);
        await conn.OpenAsync();

        // Insert rows one at a time into a chunk that will be compressed
        await conn.ExecuteAsync(@"
            INSERT INTO measurements
                (time, col_a, col_b, col_c, col_d, col_e, col_f, nullable_col)
            VALUES
                ('2025-01-01 12:00:00+00', 'aaa', 'bbb', 1, 22.5, 'eee', 'fff', 'ggg');
        ");
        await conn.ExecuteAsync(@"
            INSERT INTO measurements
                (time, col_a, col_b, col_c, col_d, col_e, col_f, nullable_col)
            VALUES
                ('2025-01-01 12:00:00+00', 'aaa', 'zzz', 1, 51.7, 'qqq', 'fff', 'hhh');
        ");
        await conn.ExecuteAsync(@"
            INSERT INTO measurements
                (time, col_a, col_b, col_c, col_d, col_e, col_f, nullable_col)
            VALUES
                ('2025-01-01 12:00:00+00', 'aaa', 'zzz', 1, -10.1, 'rrr', 'fff', 'hhh');
        ");

        int rowsBefore = await conn.ExecuteScalarAsync<int>(
            "SELECT COUNT(*) FROM measurements;");
        Assert.That(rowsBefore, Is.EqualTo(3));

        // Compress the chunk
        await conn.ExecuteAsync(@"
            SELECT compress_chunk(c, if_not_compressed => true)
            FROM show_chunks('measurements') c;
        ");

        int compressedChunks = await conn.ExecuteScalarAsync<int>(@"
            SELECT COUNT(*) FROM timescaledb_information.chunks
            WHERE hypertable_name = 'measurements' AND is_compressed = true;
        ");
        Assert.That(compressedChunks, Is.GreaterThan(0), "Chunk should be compressed");

        // Re-insert the exact same rows one at a time with ON CONFLICT DO NOTHING
        await conn.ExecuteAsync(@"
            INSERT INTO measurements
                (time, col_a, col_b, col_c, col_d, col_e, col_f, nullable_col)
            VALUES
                ('2025-01-01 12:00:00+00', 'aaa', 'bbb', 1, 22.5, 'eee', 'fff', 'ggg')
            ON CONFLICT DO NOTHING;
        ");
        await conn.ExecuteAsync(@"
            INSERT INTO measurements
                (time, col_a, col_b, col_c, col_d, col_e, col_f, nullable_col)
            VALUES
                ('2025-01-01 12:00:00+00', 'aaa', 'zzz', 1, 51.7, 'qqq', 'fff', 'hhh')
            ON CONFLICT DO NOTHING;
        ");
        await conn.ExecuteAsync(@"
            INSERT INTO measurements
                (time, col_a, col_b, col_c, col_d, col_e, col_f, nullable_col)
            VALUES
                ('2025-01-01 12:00:00+00', 'aaa', 'zzz', 1, -10.1, 'rrr', 'fff', 'hhh')
            ON CONFLICT DO NOTHING;
        ");

        int rowsAfter = await conn.ExecuteScalarAsync<int>(
            "SELECT COUNT(*) FROM measurements;");

        // This passes — ON CONFLICT DO NOTHING works correctly when nullable_col is NOT NULL.
        Assert.That(rowsAfter, Is.EqualTo(3),
            "ON CONFLICT DO NOTHING should prevent duplicates with single-row inserts");
    }
}

using Dapper.Internal;
using System;
using System.Collections.Generic;
using System.Data;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Dapper;

partial struct Command<TArgs>
{
    /// <summary>
    /// Reads buffered rows from a multi-map query with 2 types
    /// </summary>
    public List<TReturn> QueryBuffered<T1, T2, TReturn>(
        TArgs args,
        Func<T1, T2, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        string splitOn = "Id",
        int rowCountHint = 0)
    {
        SyncQueryState state = default;
        try
        {
            state.ExecuteReader(GetCommand(args), CommandBehavior.SingleResult | CommandBehavior.SequentialAccess);

            List<TReturn> results;
            if (state.Reader.Read())
            {
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var split = FindSplit(state.Reader, splitOn, tokenState1);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Tokens, split);
                
                results = RowFactory.GetRowBuffer<TReturn>(rowCountHint);
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, split, tokenState2);
                    results.Add(map(obj1, obj2));
                }
                while (state.Reader.Read());
                state.Return();
            }
            else
            {
                results = [];
            }

            // consume entire results (avoid unobserved TDS error messages)
            while (state.Reader.NextResult()) { }
            PostProcessAndRecycle(ref state, args, state.Reader.CloseAndCapture());
            return results;
        }
        finally
        {
            state.Dispose();
        }
    }

    /// <summary>
    /// Reads buffered rows from a multi-map query with 2 types (async)
    /// </summary>
    public async Task<List<TReturn>> QueryBufferedAsync<T1, T2, TReturn>(
        TArgs args,
        Func<T1, T2, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        string splitOn = "Id",
        int rowCountHint = 0,
        CancellationToken cancellationToken = default)
    {
        AsyncQueryState state = new();
        try
        {
            cancellationToken = GetCancellationToken(args, cancellationToken);
            await state.ExecuteReaderAsync(GetCommand(args), CommandBehavior.SingleResult | CommandBehavior.SequentialAccess, cancellationToken);

            List<TReturn> results;
            if (await state.Reader.ReadAsync(cancellationToken))
            {
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var split = FindSplit(state.Reader, splitOn, tokenState1);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Tokens, split);
                
                results = RowFactory.GetRowBuffer<TReturn>(rowCountHint);
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, split, tokenState2);
                    results.Add(map(obj1, obj2));
                }
                while (await state.Reader.ReadAsync(cancellationToken));
                state.Return();
            }
            else
            {
                results = [];
            }

            // consume entire results (avoid unobserved TDS error messages)
            while (await state.Reader.NextResultAsync(cancellationToken)) { }
            PostProcessAndRecycle(state, args, await state.Reader.CloseAndCaptureAsync());
            return results;
        }
        finally
        {
            await state.DisposeAsync();
        }
    }

    /// <summary>
    /// Finds the column index where the split should occur based on column name
    /// </summary>
    private static int FindSplit(System.Data.Common.DbDataReader reader, string splitOn, object tokenState)
    {
        // Default split is after finding the first occurrence of the splitOn column
        var fieldCount = reader.FieldCount;
        var splitColumns = splitOn.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        
        for (int i = 0; i < fieldCount; i++)
        {
            var name = reader.GetName(i);
            foreach (var split in splitColumns)
            {
                if (StringHashing.NormalizedEquals(name, split))
                {
                    return i;
                }
            }
        }
        
        // If not found, split in the middle
        return fieldCount / 2;
    }

    /// <summary>
    /// Finds multiple split points based on splitOn column names
    /// </summary>
    private static int[] FindSplits(System.Data.Common.DbDataReader reader, string splitOn, int count)
    {
        var splits = new int[count];
        var fieldCount = reader.FieldCount;
        var splitColumns = splitOn.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        
        int splitIndex = 0;
        int lastSplit = 0;
        
        for (int i = 0; i < fieldCount && splitIndex < count; i++)
        {
            var name = reader.GetName(i);
            foreach (var split in splitColumns)
            {
                if (StringHashing.NormalizedEquals(name, split))
                {
                    splits[splitIndex++] = i;
                    lastSplit = i;
                    break;
                }
            }
        }
        
        // Fill remaining splits evenly if not all found
        if (splitIndex < count)
        {
            var remaining = count - splitIndex;
            var step = (fieldCount - lastSplit) / (remaining + 1);
            for (int i = 0; i < remaining; i++)
            {
                lastSplit += step;
                splits[splitIndex++] = lastSplit;
            }
        }
        
        return splits;
    }

    /// <summary>
    /// Reads buffered rows from a multi-map query with 3 types
    /// </summary>
    public List<TReturn> QueryBuffered<T1, T2, T3, TReturn>(
        TArgs args,
        Func<T1, T2, T3, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        string splitOn = "Id",
        int rowCountHint = 0)
    {
        SyncQueryState state = default;
        try
        {
            state.ExecuteReader(GetCommand(args), CommandBehavior.SingleResult | CommandBehavior.SequentialAccess);

            List<TReturn> results;
            if (state.Reader.Read())
            {
                var splits = FindSplits(state.Reader, splitOn, 2);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Tokens, splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Tokens, splits[1]);
                
                results = RowFactory.GetRowBuffer<TReturn>(rowCountHint);
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    results.Add(map(obj1, obj2, obj3));
                }
                while (state.Reader.Read());
                state.Return();
            }
            else
            {
                results = [];
            }

            while (state.Reader.NextResult()) { }
            PostProcessAndRecycle(ref state, args, state.Reader.CloseAndCapture());
            return results;
        }
        finally
        {
            state.Dispose();
        }
    }

    /// <summary>
    /// Reads buffered rows from a multi-map query with 3 types (async)
    /// </summary>
    public async Task<List<TReturn>> QueryBufferedAsync<T1, T2, T3, TReturn>(
        TArgs args,
        Func<T1, T2, T3, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        string splitOn = "Id",
        int rowCountHint = 0,
        CancellationToken cancellationToken = default)
    {
        AsyncQueryState state = new();
        try
        {
            cancellationToken = GetCancellationToken(args, cancellationToken);
            await state.ExecuteReaderAsync(GetCommand(args), CommandBehavior.SingleResult | CommandBehavior.SequentialAccess, cancellationToken);

            List<TReturn> results;
            if (await state.Reader.ReadAsync(cancellationToken))
            {
                var splits = FindSplits(state.Reader, splitOn, 2);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Tokens, splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Tokens, splits[1]);
                
                results = RowFactory.GetRowBuffer<TReturn>(rowCountHint);
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    results.Add(map(obj1, obj2, obj3));
                }
                while (await state.Reader.ReadAsync(cancellationToken));
                state.Return();
            }
            else
            {
                results = [];
            }

            while (await state.Reader.NextResultAsync(cancellationToken)) { }
            PostProcessAndRecycle(state, args, await state.Reader.CloseAndCaptureAsync());
            return results;
        }
        finally
        {
            await state.DisposeAsync();
        }
    }
}

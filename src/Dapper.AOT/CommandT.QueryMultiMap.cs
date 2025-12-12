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
                var split = FindSplits(state.Reader, splitOn, 1)[0];
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), split);
                
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
                var splits = FindSplits(state.Reader, splitOn, 1);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                
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
    /// Finds multiple split points based on splitOn column names
    /// </summary>
    private static int[] FindSplits(System.Data.Common.DbDataReader reader, string splitOn, int count)
    {
        var splits = new int[count];
        var fieldCount = reader.FieldCount;
#if NET5_0_OR_GREATER
        var splitColumns = splitOn.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
#else
        var splitColumns = splitOn.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
        for (int idx = 0; idx < splitColumns.Length; idx++)
        {
            splitColumns[idx] = splitColumns[idx].Trim();
        }
#endif
        
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
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                
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
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                
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

    /// <summary>
    /// Reads buffered rows from a multi-map query with 4 types
    /// </summary>
    public List<TReturn> QueryBuffered<T1, T2, T3, T4, TReturn>(
        TArgs args,
        Func<T1, T2, T3, T4, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        [DapperAot] RowFactory<T4>? factory4 = null,
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
                var splits = FindSplits(state.Reader, splitOn, 3);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                var tokenState4 = (factory4 ??= RowFactory<T4>.Default).Tokenize(state.Reader, state.Lease(), splits[2]);
                
                results = RowFactory.GetRowBuffer<TReturn>(rowCountHint);
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    var obj4 = factory4.Read(state.Reader, state.Tokens, splits[2], tokenState4);
                    results.Add(map(obj1, obj2, obj3, obj4));
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
    /// Reads buffered rows from a multi-map query with 4 types (async)
    /// </summary>
    public async Task<List<TReturn>> QueryBufferedAsync<T1, T2, T3, T4, TReturn>(
        TArgs args,
        Func<T1, T2, T3, T4, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        [DapperAot] RowFactory<T4>? factory4 = null,
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
                var splits = FindSplits(state.Reader, splitOn, 3);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                var tokenState4 = (factory4 ??= RowFactory<T4>.Default).Tokenize(state.Reader, state.Lease(), splits[2]);
                
                results = RowFactory.GetRowBuffer<TReturn>(rowCountHint);
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    var obj4 = factory4.Read(state.Reader, state.Tokens, splits[2], tokenState4);
                    results.Add(map(obj1, obj2, obj3, obj4));
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

    /// <summary>
    /// Reads buffered rows from a multi-map query with 5 types
    /// </summary>
    public List<TReturn> QueryBuffered<T1, T2, T3, T4, T5, TReturn>(
        TArgs args,
        Func<T1, T2, T3, T4, T5, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        [DapperAot] RowFactory<T4>? factory4 = null,
        [DapperAot] RowFactory<T5>? factory5 = null,
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
                var splits = FindSplits(state.Reader, splitOn, 4);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                var tokenState4 = (factory4 ??= RowFactory<T4>.Default).Tokenize(state.Reader, state.Lease(), splits[2]);
                var tokenState5 = (factory5 ??= RowFactory<T5>.Default).Tokenize(state.Reader, state.Lease(), splits[3]);
                
                results = RowFactory.GetRowBuffer<TReturn>(rowCountHint);
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    var obj4 = factory4.Read(state.Reader, state.Tokens, splits[2], tokenState4);
                    var obj5 = factory5.Read(state.Reader, state.Tokens, splits[3], tokenState5);
                    results.Add(map(obj1, obj2, obj3, obj4, obj5));
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
    /// Reads buffered rows from a multi-map query with 5 types (async)
    /// </summary>
    public async Task<List<TReturn>> QueryBufferedAsync<T1, T2, T3, T4, T5, TReturn>(
        TArgs args,
        Func<T1, T2, T3, T4, T5, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        [DapperAot] RowFactory<T4>? factory4 = null,
        [DapperAot] RowFactory<T5>? factory5 = null,
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
                var splits = FindSplits(state.Reader, splitOn, 4);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                var tokenState4 = (factory4 ??= RowFactory<T4>.Default).Tokenize(state.Reader, state.Lease(), splits[2]);
                var tokenState5 = (factory5 ??= RowFactory<T5>.Default).Tokenize(state.Reader, state.Lease(), splits[3]);
                
                results = RowFactory.GetRowBuffer<TReturn>(rowCountHint);
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    var obj4 = factory4.Read(state.Reader, state.Tokens, splits[2], tokenState4);
                    var obj5 = factory5.Read(state.Reader, state.Tokens, splits[3], tokenState5);
                    results.Add(map(obj1, obj2, obj3, obj4, obj5));
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

    /// <summary>
    /// Reads buffered rows from a multi-map query with 6 types
    /// </summary>
    public List<TReturn> QueryBuffered<T1, T2, T3, T4, T5, T6, TReturn>(
        TArgs args,
        Func<T1, T2, T3, T4, T5, T6, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        [DapperAot] RowFactory<T4>? factory4 = null,
        [DapperAot] RowFactory<T5>? factory5 = null,
        [DapperAot] RowFactory<T6>? factory6 = null,
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
                var splits = FindSplits(state.Reader, splitOn, 5);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                var tokenState4 = (factory4 ??= RowFactory<T4>.Default).Tokenize(state.Reader, state.Lease(), splits[2]);
                var tokenState5 = (factory5 ??= RowFactory<T5>.Default).Tokenize(state.Reader, state.Lease(), splits[3]);
                var tokenState6 = (factory6 ??= RowFactory<T6>.Default).Tokenize(state.Reader, state.Lease(), splits[4]);
                
                results = RowFactory.GetRowBuffer<TReturn>(rowCountHint);
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    var obj4 = factory4.Read(state.Reader, state.Tokens, splits[2], tokenState4);
                    var obj5 = factory5.Read(state.Reader, state.Tokens, splits[3], tokenState5);
                    var obj6 = factory6.Read(state.Reader, state.Tokens, splits[4], tokenState6);
                    results.Add(map(obj1, obj2, obj3, obj4, obj5, obj6));
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
    /// Reads buffered rows from a multi-map query with 6 types (async)
    /// </summary>
    public async Task<List<TReturn>> QueryBufferedAsync<T1, T2, T3, T4, T5, T6, TReturn>(
        TArgs args,
        Func<T1, T2, T3, T4, T5, T6, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        [DapperAot] RowFactory<T4>? factory4 = null,
        [DapperAot] RowFactory<T5>? factory5 = null,
        [DapperAot] RowFactory<T6>? factory6 = null,
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
                var splits = FindSplits(state.Reader, splitOn, 5);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                var tokenState4 = (factory4 ??= RowFactory<T4>.Default).Tokenize(state.Reader, state.Lease(), splits[2]);
                var tokenState5 = (factory5 ??= RowFactory<T5>.Default).Tokenize(state.Reader, state.Lease(), splits[3]);
                var tokenState6 = (factory6 ??= RowFactory<T6>.Default).Tokenize(state.Reader, state.Lease(), splits[4]);
                
                results = RowFactory.GetRowBuffer<TReturn>(rowCountHint);
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    var obj4 = factory4.Read(state.Reader, state.Tokens, splits[2], tokenState4);
                    var obj5 = factory5.Read(state.Reader, state.Tokens, splits[3], tokenState5);
                    var obj6 = factory6.Read(state.Reader, state.Tokens, splits[4], tokenState6);
                    results.Add(map(obj1, obj2, obj3, obj4, obj5, obj6));
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

    /// <summary>
    /// Reads buffered rows from a multi-map query with 7 types
    /// </summary>
    public List<TReturn> QueryBuffered<T1, T2, T3, T4, T5, T6, T7, TReturn>(
        TArgs args,
        Func<T1, T2, T3, T4, T5, T6, T7, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        [DapperAot] RowFactory<T4>? factory4 = null,
        [DapperAot] RowFactory<T5>? factory5 = null,
        [DapperAot] RowFactory<T6>? factory6 = null,
        [DapperAot] RowFactory<T7>? factory7 = null,
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
                var splits = FindSplits(state.Reader, splitOn, 6);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                var tokenState4 = (factory4 ??= RowFactory<T4>.Default).Tokenize(state.Reader, state.Lease(), splits[2]);
                var tokenState5 = (factory5 ??= RowFactory<T5>.Default).Tokenize(state.Reader, state.Lease(), splits[3]);
                var tokenState6 = (factory6 ??= RowFactory<T6>.Default).Tokenize(state.Reader, state.Lease(), splits[4]);
                var tokenState7 = (factory7 ??= RowFactory<T7>.Default).Tokenize(state.Reader, state.Lease(), splits[5]);
                
                results = RowFactory.GetRowBuffer<TReturn>(rowCountHint);
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    var obj4 = factory4.Read(state.Reader, state.Tokens, splits[2], tokenState4);
                    var obj5 = factory5.Read(state.Reader, state.Tokens, splits[3], tokenState5);
                    var obj6 = factory6.Read(state.Reader, state.Tokens, splits[4], tokenState6);
                    var obj7 = factory7.Read(state.Reader, state.Tokens, splits[5], tokenState7);
                    results.Add(map(obj1, obj2, obj3, obj4, obj5, obj6, obj7));
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
    /// Reads buffered rows from a multi-map query with 7 types (async)
    /// </summary>
    public async Task<List<TReturn>> QueryBufferedAsync<T1, T2, T3, T4, T5, T6, T7, TReturn>(
        TArgs args,
        Func<T1, T2, T3, T4, T5, T6, T7, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        [DapperAot] RowFactory<T4>? factory4 = null,
        [DapperAot] RowFactory<T5>? factory5 = null,
        [DapperAot] RowFactory<T6>? factory6 = null,
        [DapperAot] RowFactory<T7>? factory7 = null,
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
                var splits = FindSplits(state.Reader, splitOn, 6);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                var tokenState4 = (factory4 ??= RowFactory<T4>.Default).Tokenize(state.Reader, state.Lease(), splits[2]);
                var tokenState5 = (factory5 ??= RowFactory<T5>.Default).Tokenize(state.Reader, state.Lease(), splits[3]);
                var tokenState6 = (factory6 ??= RowFactory<T6>.Default).Tokenize(state.Reader, state.Lease(), splits[4]);
                var tokenState7 = (factory7 ??= RowFactory<T7>.Default).Tokenize(state.Reader, state.Lease(), splits[5]);
                
                results = RowFactory.GetRowBuffer<TReturn>(rowCountHint);
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    var obj4 = factory4.Read(state.Reader, state.Tokens, splits[2], tokenState4);
                    var obj5 = factory5.Read(state.Reader, state.Tokens, splits[3], tokenState5);
                    var obj6 = factory6.Read(state.Reader, state.Tokens, splits[4], tokenState6);
                    var obj7 = factory7.Read(state.Reader, state.Tokens, splits[5], tokenState7);
                    results.Add(map(obj1, obj2, obj3, obj4, obj5, obj6, obj7));
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

    /// <summary>
    /// Reads unbuffered rows from a multi-map query with 2 types
    /// </summary>
    public IEnumerable<TReturn> QueryUnbuffered<T1, T2, TReturn>(
        TArgs args,
        Func<T1, T2, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        string splitOn = "Id")
    {
        SyncQueryState state = default;
        try
        {
            state.ExecuteReader(GetCommand(args), CommandBehavior.SingleResult | CommandBehavior.SequentialAccess);

            if (state.Reader.Read())
            {
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var split = FindSplits(state.Reader, splitOn, 1)[0];
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), split);
                
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, split, tokenState2);
                    yield return map(obj1, obj2);
                }
                while (state.Reader.Read());
                state.Return();
            }

            while (state.Reader.NextResult()) { }
            PostProcessAndRecycle(ref state, args, state.Reader.CloseAndCapture());
        }
        finally
        {
            state.Dispose();
        }
    }

    /// <summary>
    /// Reads unbuffered rows from a multi-map query with 2 types (async)
    /// </summary>
    public async IAsyncEnumerable<TReturn> QueryUnbufferedAsync<T1, T2, TReturn>(
        TArgs args,
        Func<T1, T2, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        string splitOn = "Id",
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        AsyncQueryState state = new();
        try
        {
            cancellationToken = GetCancellationToken(args, cancellationToken);
            await state.ExecuteReaderAsync(GetCommand(args), CommandBehavior.SingleResult | CommandBehavior.SequentialAccess, cancellationToken);

            if (await state.Reader.ReadAsync(cancellationToken))
            {
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var split = FindSplits(state.Reader, splitOn, 1)[0];
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), split);
                
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, split, tokenState2);
                    yield return map(obj1, obj2);
                }
                while (await state.Reader.ReadAsync(cancellationToken));
                state.Return();
            }

            while (await state.Reader.NextResultAsync(cancellationToken)) { }
            PostProcessAndRecycle(state, args, await state.Reader.CloseAndCaptureAsync());
        }
        finally
        {
            await state.DisposeAsync();
        }
    }

    /// <summary>
    /// Reads unbuffered rows from a multi-map query with 3 types
    /// </summary>
    public IEnumerable<TReturn> QueryUnbuffered<T1, T2, T3, TReturn>(
        TArgs args,
        Func<T1, T2, T3, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        string splitOn = "Id")
    {
        SyncQueryState state = default;
        try
        {
            state.ExecuteReader(GetCommand(args), CommandBehavior.SingleResult | CommandBehavior.SequentialAccess);

            if (state.Reader.Read())
            {
                var splits = FindSplits(state.Reader, splitOn, 2);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    yield return map(obj1, obj2, obj3);
                }
                while (state.Reader.Read());
                state.Return();
            }

            while (state.Reader.NextResult()) { }
            PostProcessAndRecycle(ref state, args, state.Reader.CloseAndCapture());
        }
        finally
        {
            state.Dispose();
        }
    }

    /// <summary>
    /// Reads unbuffered rows from a multi-map query with 3 types (async)
    /// </summary>
    public async IAsyncEnumerable<TReturn> QueryUnbufferedAsync<T1, T2, T3, TReturn>(
        TArgs args,
        Func<T1, T2, T3, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        string splitOn = "Id",
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        AsyncQueryState state = new();
        try
        {
            cancellationToken = GetCancellationToken(args, cancellationToken);
            await state.ExecuteReaderAsync(GetCommand(args), CommandBehavior.SingleResult | CommandBehavior.SequentialAccess, cancellationToken);

            if (await state.Reader.ReadAsync(cancellationToken))
            {
                var splits = FindSplits(state.Reader, splitOn, 2);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    yield return map(obj1, obj2, obj3);
                }
                while (await state.Reader.ReadAsync(cancellationToken));
                state.Return();
            }

            while (await state.Reader.NextResultAsync(cancellationToken)) { }
            PostProcessAndRecycle(state, args, await state.Reader.CloseAndCaptureAsync());
        }
        finally
        {
            await state.DisposeAsync();
        }
    }

    /// <summary>
    /// Reads unbuffered rows from a multi-map query with 4 types
    /// </summary>
    public IEnumerable<TReturn> QueryUnbuffered<T1, T2, T3, T4, TReturn>(
        TArgs args,
        Func<T1, T2, T3, T4, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        [DapperAot] RowFactory<T4>? factory4 = null,
        string splitOn = "Id")
    {
        SyncQueryState state = default;
        try
        {
            state.ExecuteReader(GetCommand(args), CommandBehavior.SingleResult | CommandBehavior.SequentialAccess);

            if (state.Reader.Read())
            {
                var splits = FindSplits(state.Reader, splitOn, 3);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                var tokenState4 = (factory4 ??= RowFactory<T4>.Default).Tokenize(state.Reader, state.Lease(), splits[2]);
                
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    var obj4 = factory4.Read(state.Reader, state.Tokens, splits[2], tokenState4);
                    yield return map(obj1, obj2, obj3, obj4);
                }
                while (state.Reader.Read());
                state.Return();
            }

            while (state.Reader.NextResult()) { }
            PostProcessAndRecycle(ref state, args, state.Reader.CloseAndCapture());
        }
        finally
        {
            state.Dispose();
        }
    }

    /// <summary>
    /// Reads unbuffered rows from a multi-map query with 4 types (async)
    /// </summary>
    public async IAsyncEnumerable<TReturn> QueryUnbufferedAsync<T1, T2, T3, T4, TReturn>(
        TArgs args,
        Func<T1, T2, T3, T4, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        [DapperAot] RowFactory<T4>? factory4 = null,
        string splitOn = "Id",
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        AsyncQueryState state = new();
        try
        {
            cancellationToken = GetCancellationToken(args, cancellationToken);
            await state.ExecuteReaderAsync(GetCommand(args), CommandBehavior.SingleResult | CommandBehavior.SequentialAccess, cancellationToken);

            if (await state.Reader.ReadAsync(cancellationToken))
            {
                var splits = FindSplits(state.Reader, splitOn, 3);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                var tokenState4 = (factory4 ??= RowFactory<T4>.Default).Tokenize(state.Reader, state.Lease(), splits[2]);
                
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    var obj4 = factory4.Read(state.Reader, state.Tokens, splits[2], tokenState4);
                    yield return map(obj1, obj2, obj3, obj4);
                }
                while (await state.Reader.ReadAsync(cancellationToken));
                state.Return();
            }

            while (await state.Reader.NextResultAsync(cancellationToken)) { }
            PostProcessAndRecycle(state, args, await state.Reader.CloseAndCaptureAsync());
        }
        finally
        {
            await state.DisposeAsync();
        }
    }

    /// <summary>
    /// Reads unbuffered rows from a multi-map query with 5 types
    /// </summary>
    public IEnumerable<TReturn> QueryUnbuffered<T1, T2, T3, T4, T5, TReturn>(
        TArgs args,
        Func<T1, T2, T3, T4, T5, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        [DapperAot] RowFactory<T4>? factory4 = null,
        [DapperAot] RowFactory<T5>? factory5 = null,
        string splitOn = "Id")
    {
        SyncQueryState state = default;
        try
        {
            state.ExecuteReader(GetCommand(args), CommandBehavior.SingleResult | CommandBehavior.SequentialAccess);

            if (state.Reader.Read())
            {
                var splits = FindSplits(state.Reader, splitOn, 4);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                var tokenState4 = (factory4 ??= RowFactory<T4>.Default).Tokenize(state.Reader, state.Lease(), splits[2]);
                var tokenState5 = (factory5 ??= RowFactory<T5>.Default).Tokenize(state.Reader, state.Lease(), splits[3]);
                
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    var obj4 = factory4.Read(state.Reader, state.Tokens, splits[2], tokenState4);
                    var obj5 = factory5.Read(state.Reader, state.Tokens, splits[3], tokenState5);
                    yield return map(obj1, obj2, obj3, obj4, obj5);
                }
                while (state.Reader.Read());
                state.Return();
            }

            while (state.Reader.NextResult()) { }
            PostProcessAndRecycle(ref state, args, state.Reader.CloseAndCapture());
        }
        finally
        {
            state.Dispose();
        }
    }

    /// <summary>
    /// Reads unbuffered rows from a multi-map query with 5 types (async)
    /// </summary>
    public async IAsyncEnumerable<TReturn> QueryUnbufferedAsync<T1, T2, T3, T4, T5, TReturn>(
        TArgs args,
        Func<T1, T2, T3, T4, T5, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        [DapperAot] RowFactory<T4>? factory4 = null,
        [DapperAot] RowFactory<T5>? factory5 = null,
        string splitOn = "Id",
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        AsyncQueryState state = new();
        try
        {
            cancellationToken = GetCancellationToken(args, cancellationToken);
            await state.ExecuteReaderAsync(GetCommand(args), CommandBehavior.SingleResult | CommandBehavior.SequentialAccess, cancellationToken);

            if (await state.Reader.ReadAsync(cancellationToken))
            {
                var splits = FindSplits(state.Reader, splitOn, 4);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                var tokenState4 = (factory4 ??= RowFactory<T4>.Default).Tokenize(state.Reader, state.Lease(), splits[2]);
                var tokenState5 = (factory5 ??= RowFactory<T5>.Default).Tokenize(state.Reader, state.Lease(), splits[3]);
                
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    var obj4 = factory4.Read(state.Reader, state.Tokens, splits[2], tokenState4);
                    var obj5 = factory5.Read(state.Reader, state.Tokens, splits[3], tokenState5);
                    yield return map(obj1, obj2, obj3, obj4, obj5);
                }
                while (await state.Reader.ReadAsync(cancellationToken));
                state.Return();
            }

            while (await state.Reader.NextResultAsync(cancellationToken)) { }
            PostProcessAndRecycle(state, args, await state.Reader.CloseAndCaptureAsync());
        }
        finally
        {
            await state.DisposeAsync();
        }
    }

    /// <summary>
    /// Reads unbuffered rows from a multi-map query with 6 types
    /// </summary>
    public IEnumerable<TReturn> QueryUnbuffered<T1, T2, T3, T4, T5, T6, TReturn>(
        TArgs args,
        Func<T1, T2, T3, T4, T5, T6, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        [DapperAot] RowFactory<T4>? factory4 = null,
        [DapperAot] RowFactory<T5>? factory5 = null,
        [DapperAot] RowFactory<T6>? factory6 = null,
        string splitOn = "Id")
    {
        SyncQueryState state = default;
        try
        {
            state.ExecuteReader(GetCommand(args), CommandBehavior.SingleResult | CommandBehavior.SequentialAccess);

            if (state.Reader.Read())
            {
                var splits = FindSplits(state.Reader, splitOn, 5);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                var tokenState4 = (factory4 ??= RowFactory<T4>.Default).Tokenize(state.Reader, state.Lease(), splits[2]);
                var tokenState5 = (factory5 ??= RowFactory<T5>.Default).Tokenize(state.Reader, state.Lease(), splits[3]);
                var tokenState6 = (factory6 ??= RowFactory<T6>.Default).Tokenize(state.Reader, state.Lease(), splits[4]);
                
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    var obj4 = factory4.Read(state.Reader, state.Tokens, splits[2], tokenState4);
                    var obj5 = factory5.Read(state.Reader, state.Tokens, splits[3], tokenState5);
                    var obj6 = factory6.Read(state.Reader, state.Tokens, splits[4], tokenState6);
                    yield return map(obj1, obj2, obj3, obj4, obj5, obj6);
                }
                while (state.Reader.Read());
                state.Return();
            }

            while (state.Reader.NextResult()) { }
            PostProcessAndRecycle(ref state, args, state.Reader.CloseAndCapture());
        }
        finally
        {
            state.Dispose();
        }
    }

    /// <summary>
    /// Reads unbuffered rows from a multi-map query with 6 types (async)
    /// </summary>
    public async IAsyncEnumerable<TReturn> QueryUnbufferedAsync<T1, T2, T3, T4, T5, T6, TReturn>(
        TArgs args,
        Func<T1, T2, T3, T4, T5, T6, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        [DapperAot] RowFactory<T4>? factory4 = null,
        [DapperAot] RowFactory<T5>? factory5 = null,
        [DapperAot] RowFactory<T6>? factory6 = null,
        string splitOn = "Id",
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        AsyncQueryState state = new();
        try
        {
            cancellationToken = GetCancellationToken(args, cancellationToken);
            await state.ExecuteReaderAsync(GetCommand(args), CommandBehavior.SingleResult | CommandBehavior.SequentialAccess, cancellationToken);

            if (await state.Reader.ReadAsync(cancellationToken))
            {
                var splits = FindSplits(state.Reader, splitOn, 5);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                var tokenState4 = (factory4 ??= RowFactory<T4>.Default).Tokenize(state.Reader, state.Lease(), splits[2]);
                var tokenState5 = (factory5 ??= RowFactory<T5>.Default).Tokenize(state.Reader, state.Lease(), splits[3]);
                var tokenState6 = (factory6 ??= RowFactory<T6>.Default).Tokenize(state.Reader, state.Lease(), splits[4]);
                
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    var obj4 = factory4.Read(state.Reader, state.Tokens, splits[2], tokenState4);
                    var obj5 = factory5.Read(state.Reader, state.Tokens, splits[3], tokenState5);
                    var obj6 = factory6.Read(state.Reader, state.Tokens, splits[4], tokenState6);
                    yield return map(obj1, obj2, obj3, obj4, obj5, obj6);
                }
                while (await state.Reader.ReadAsync(cancellationToken));
                state.Return();
            }

            while (await state.Reader.NextResultAsync(cancellationToken)) { }
            PostProcessAndRecycle(state, args, await state.Reader.CloseAndCaptureAsync());
        }
        finally
        {
            await state.DisposeAsync();
        }
    }

    /// <summary>
    /// Reads unbuffered rows from a multi-map query with 7 types
    /// </summary>
    public IEnumerable<TReturn> QueryUnbuffered<T1, T2, T3, T4, T5, T6, T7, TReturn>(
        TArgs args,
        Func<T1, T2, T3, T4, T5, T6, T7, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        [DapperAot] RowFactory<T4>? factory4 = null,
        [DapperAot] RowFactory<T5>? factory5 = null,
        [DapperAot] RowFactory<T6>? factory6 = null,
        [DapperAot] RowFactory<T7>? factory7 = null,
        string splitOn = "Id")
    {
        SyncQueryState state = default;
        try
        {
            state.ExecuteReader(GetCommand(args), CommandBehavior.SingleResult | CommandBehavior.SequentialAccess);

            if (state.Reader.Read())
            {
                var splits = FindSplits(state.Reader, splitOn, 6);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                var tokenState4 = (factory4 ??= RowFactory<T4>.Default).Tokenize(state.Reader, state.Lease(), splits[2]);
                var tokenState5 = (factory5 ??= RowFactory<T5>.Default).Tokenize(state.Reader, state.Lease(), splits[3]);
                var tokenState6 = (factory6 ??= RowFactory<T6>.Default).Tokenize(state.Reader, state.Lease(), splits[4]);
                var tokenState7 = (factory7 ??= RowFactory<T7>.Default).Tokenize(state.Reader, state.Lease(), splits[5]);
                
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    var obj4 = factory4.Read(state.Reader, state.Tokens, splits[2], tokenState4);
                    var obj5 = factory5.Read(state.Reader, state.Tokens, splits[3], tokenState5);
                    var obj6 = factory6.Read(state.Reader, state.Tokens, splits[4], tokenState6);
                    var obj7 = factory7.Read(state.Reader, state.Tokens, splits[5], tokenState7);
                    yield return map(obj1, obj2, obj3, obj4, obj5, obj6, obj7);
                }
                while (state.Reader.Read());
                state.Return();
            }

            while (state.Reader.NextResult()) { }
            PostProcessAndRecycle(ref state, args, state.Reader.CloseAndCapture());
        }
        finally
        {
            state.Dispose();
        }
    }

    /// <summary>
    /// Reads unbuffered rows from a multi-map query with 7 types (async)
    /// </summary>
    public async IAsyncEnumerable<TReturn> QueryUnbufferedAsync<T1, T2, T3, T4, T5, T6, T7, TReturn>(
        TArgs args,
        Func<T1, T2, T3, T4, T5, T6, T7, TReturn> map,
        [DapperAot] RowFactory<T1>? factory1 = null,
        [DapperAot] RowFactory<T2>? factory2 = null,
        [DapperAot] RowFactory<T3>? factory3 = null,
        [DapperAot] RowFactory<T4>? factory4 = null,
        [DapperAot] RowFactory<T5>? factory5 = null,
        [DapperAot] RowFactory<T6>? factory6 = null,
        [DapperAot] RowFactory<T7>? factory7 = null,
        string splitOn = "Id",
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        AsyncQueryState state = new();
        try
        {
            cancellationToken = GetCancellationToken(args, cancellationToken);
            await state.ExecuteReaderAsync(GetCommand(args), CommandBehavior.SingleResult | CommandBehavior.SequentialAccess, cancellationToken);

            if (await state.Reader.ReadAsync(cancellationToken))
            {
                var splits = FindSplits(state.Reader, splitOn, 6);
                var tokenState1 = (factory1 ??= RowFactory<T1>.Default).Tokenize(state.Reader, state.Lease(), 0);
                var tokenState2 = (factory2 ??= RowFactory<T2>.Default).Tokenize(state.Reader, state.Lease(), splits[0]);
                var tokenState3 = (factory3 ??= RowFactory<T3>.Default).Tokenize(state.Reader, state.Lease(), splits[1]);
                var tokenState4 = (factory4 ??= RowFactory<T4>.Default).Tokenize(state.Reader, state.Lease(), splits[2]);
                var tokenState5 = (factory5 ??= RowFactory<T5>.Default).Tokenize(state.Reader, state.Lease(), splits[3]);
                var tokenState6 = (factory6 ??= RowFactory<T6>.Default).Tokenize(state.Reader, state.Lease(), splits[4]);
                var tokenState7 = (factory7 ??= RowFactory<T7>.Default).Tokenize(state.Reader, state.Lease(), splits[5]);
                
                do
                {
                    var obj1 = factory1.Read(state.Reader, state.Tokens, 0, tokenState1);
                    var obj2 = factory2.Read(state.Reader, state.Tokens, splits[0], tokenState2);
                    var obj3 = factory3.Read(state.Reader, state.Tokens, splits[1], tokenState3);
                    var obj4 = factory4.Read(state.Reader, state.Tokens, splits[2], tokenState4);
                    var obj5 = factory5.Read(state.Reader, state.Tokens, splits[3], tokenState5);
                    var obj6 = factory6.Read(state.Reader, state.Tokens, splits[4], tokenState6);
                    var obj7 = factory7.Read(state.Reader, state.Tokens, splits[5], tokenState7);
                    yield return map(obj1, obj2, obj3, obj4, obj5, obj6, obj7);
                }
                while (await state.Reader.ReadAsync(cancellationToken));
                state.Return();
            }

            while (await state.Reader.NextResultAsync(cancellationToken)) { }
            PostProcessAndRecycle(state, args, await state.Reader.CloseAndCaptureAsync());
        }
        finally
        {
            await state.DisposeAsync();
        }
    }
}

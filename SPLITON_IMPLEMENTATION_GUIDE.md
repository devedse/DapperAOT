# splitOn Implementation Guide for DapperAOT

## Overview
This document outlines the implementation plan for adding full splitOn support to DapperAOT. The splitOn feature allows multi-type mapping where a single database row can be split into multiple objects based on column boundaries.

## Current Status
- ✅ Added `MultiType` operation flag to identify multi-type queries
- ✅ Removed `NotAotSupported` flag for multi-type Query methods  
- ✅ Added "map" and "splitOn" parameter recognition in analyzer
- ✅ Updated tests to not expect errors for multi-type queries
- ⚠️ Multi-type queries currently fall back to vanilla Dapper (DoNotGenerate flag set)

## Architecture Requirements

### 1. Analyzer Changes (`src/Dapper.AOT.Analyzers/`)

#### SharedParseArgsAndFlags Enhancement
**File:** `CodeAnalysis/DapperAnalyzer.cs`

Need to parse and validate:
- `splitOn` parameter value (string, default "Id")
- `map` function delegate type and parameters
- Type arguments from Query<T1, T2, ..., TReturn>

```csharp
// Parse splitOn parameter
case "splitOn":
    if (TryGetConstantValue(arg, out string? splitOnValue))
    {
        // Store splitOn value for code generation
        // Validate format (comma-separated column names)
    }
    break;

// Parse map function  
case "map":
    // Extract delegate type
    // Validate: Func<T1, T2, ..., TReturn> matches Query<T1, T2, ..., TReturn>
    // Store map function signature
    break;
```

#### Type Extraction
Extract type arguments from IMethodSymbol:
```csharp
var typeArgs = method.TypeArguments;
// For Query<Product, Category, Product>:
// typeArgs[0] = Product (T1)
// typeArgs[1] = Category (T2) 
// typeArgs[2] = Product (TReturn)
```

### 2. Code Generator Changes (`src/Dapper.AOT.Analyzers/CodeAnalysis/`)

#### DapperInterceptorGenerator.cs
Remove the temporary DoNotGenerate check for MultiType:
```csharp
// TODO: Remove this when full implementation is complete
if (flags.HasAny(OperationFlags.MultiType))
{
    flags |= OperationFlags.DoNotGenerate;
    return null;
}
```

Add multi-type generation path:
```csharp
if (flags.HasAny(OperationFlags.MultiType))
{
    WriteMultiTypeImplementation(sb, method, flags, ...);
}
else
{
    WriteSingleImplementation(sb, method, ...);
}
```

#### New File: DapperInterceptorGenerator.MultiType.cs
Create specialized multi-type code generation:

```csharp
static void WriteMultiTypeImplementation(
    CodeWriter sb,
    IMethodSymbol method,
    OperationFlags flags,
    ImmutableArray<ITypeSymbol> typeArgs,  // T1, T2, ..., TReturn
    string splitOn,
    ...)
{
    // Generate:
    // 1. Multi-type row factory
    // 2. Column splitting logic based on splitOn
    // 3. Map function invocation
    // 4. Return mapped results
}
```

Example generated code structure:
```csharp
internal static IEnumerable<Product> Query0(
    this IDbConnection cnn, 
    string sql,
    Func<Product, Category, Product> map,
    ...,
    string splitOn)
{
    return Command(cnn, transaction, sql, ...)
        .QueryBuffered(param, new MultiTypeRowFactory<Product, Category, Product>(
            RowFactory0.Instance,  // Product factory
            RowFactory1.Instance,  // Category factory  
            map,
            splitOn));
}
```

### 3. Runtime Library Changes (`src/Dapper.AOT/`)

#### New File: MultiTypeRowFactory.cs
Create row factory for multi-type scenarios:

```csharp
public class MultiTypeRowFactory<T1, T2, TReturn> : RowFactory<TReturn>
{
    private readonly RowFactory<T1> _factory1;
    private readonly RowFactory<T2> _factory2;
    private readonly Func<T1, T2, TReturn> _map;
    private readonly string _splitOn;
    private int _splitIndex = -1;

    public override object? Tokenize(DbDataReader reader, Span<int> tokens, int columnOffset)
    {
        // Find split column index by name
        if (_splitIndex == -1)
        {
            _splitIndex = FindSplitIndex(reader, _splitOn);
        }

        // Tokenize first type (columns 0 to splitIndex-1)
        var state1 = _factory1.Tokenize(reader, tokens.Slice(0, _splitIndex), 0);
        
        // Tokenize second type (columns splitIndex to end)
        var state2 = _factory2.Tokenize(reader, tokens.Slice(_splitIndex), _splitIndex);
        
        return new MultiTypeState { State1 = state1, State2 = state2 };
    }

    public override TReturn Read(DbDataReader reader, ReadOnlySpan<int> tokens, int columnOffset, object? state)
    {
        var multiState = (MultiTypeState)state;
        
        // Read first object
        var obj1 = _factory1.Read(reader, tokens.Slice(0, _splitIndex), 0, multiState.State1);
        
        // Read second object  
        var obj2 = _factory2.Read(reader, tokens.Slice(_splitIndex), _splitIndex, multiState.State2);
        
        // Invoke map function
        return _map(obj1, obj2);
    }

    private int FindSplitIndex(DbDataReader reader, string splitOn)
    {
        // Parse comma-separated splitOn values
        var splitColumns = splitOn.Split(',');
        
        for (int i = 0; i < reader.FieldCount; i++)
        {
            var columnName = reader.GetName(i);
            if (splitColumns.Any(s => NormalizedEquals(columnName, s.Trim())))
            {
                return i;
            }
        }
        
        // Default to "Id" if not found
        for (int i = 0; i < reader.FieldCount; i++)
        {
            if (NormalizedEquals(reader.GetName(i), "Id"))
            {
                return i;
            }
        }
        
        throw new InvalidOperationException($"Split column '{splitOn}' not found");
    }
}
```

#### Support Multiple Arities
Create factories for each supported arity (2-7 types):
- `MultiTypeRowFactory<T1, T2, TReturn>`
- `MultiTypeRowFactory<T1, T2, T3, TReturn>`
- `MultiTypeRowFactory<T1, T2, T3, T4, TReturn>`
- ... up to 7 types

### 4. Testing Requirements

#### Interceptor Tests
Create comprehensive test files in `test/Dapper.AOT.Test/Interceptors/`:

**QuerySplitOn2Types.input.cs:**
```csharp
[module: DapperAot]
public static class SplitOnTests
{
    static void TwoTypes(DbConnection cnn)
    {
        // Default splitOn
        _ = cnn.Query<Product, Category, Product>(
            "SELECT * FROM Products p JOIN Categories c ON p.CategoryId = c.Id",
            (p, c) => { p.Category = c; return p; });
        
        // Explicit splitOn
        _ = cnn.Query<Product, Category, Product>(
            "SELECT * FROM Products p JOIN Categories c ON p.CategoryId = c.Id",
            (p, c) => { p.Category = c; return p; },
            splitOn: "CategoryId");
    }
}
```

**QuerySplitOn3Types.input.cs, QuerySplitOn4Types.input.cs, etc.**

#### Integration Tests  
Add to `test/Dapper.AOT.Test.Integration/`:

```csharp
[Fact]
public void QueryWithSplitOn_TwoTypes()
{
    var sql = @"
        SELECT 1 as ProductId, 'Widget' as ProductName, 
               10 as CategoryId, 'Tools' as CategoryName";
               
    var results = connection.Query<Product, Category, Product>(
        sql,
        (product, category) => { 
            product.Category = category; 
            return product; 
        },
        splitOn: "CategoryId");
        
    var product = Assert.Single(results);
    Assert.Equal(1, product.ProductId);
    Assert.Equal("Widget", product.ProductName);
    Assert.NotNull(product.Category);
    Assert.Equal(10, product.Category.CategoryId);
    Assert.Equal("Tools", product.Category.CategoryName);
}
```

### 5. Edge Cases to Handle

1. **Invalid splitOn column**: Column doesn't exist in result set
2. **Multiple splitOn columns**: "Id,Name" for 3+ types
3. **Null handling**: What if split column is NULL?
4. **Column ordering**: Split columns must appear in order
5. **Async variants**: QueryAsync<T1, T2, TReturn>
6. **Buffered vs Unbuffered**: Both modes must work
7. **Single row queries**: QueryFirst/Single with multi-type
8. **Parameter binding**: Works with parameterized queries

### 6. Performance Considerations

- Column index lookup should be cached (done once per reader)
- Token allocation should use stack allocation when possible
- Minimize delegate allocations
- Consider SIMD optimizations for column searching

### 7. Documentation Updates

Update documentation in `docs/`:
- Add splitOn examples to getting started guide
- Document supported scenarios and limitations
- Add FAQ entry about multi-mapping
- Create troubleshooting guide for common issues

## Implementation Phases

### Phase 1: Foundation (COMPLETED)
- ✅ Add MultiType flag
- ✅ Parse map and splitOn parameters
- ✅ Update tests

### Phase 2: Two-Type Support (NEXT)
- [ ] Implement MultiTypeRowFactory<T1, T2, TReturn>
- [ ] Generate code for 2-type queries
- [ ] Add interceptor tests for 2-type scenarios
- [ ] Add integration tests

### Phase 3: Multi-Type Support (3-7 types)
- [ ] Implement factories for arities 3-7
- [ ] Generate code for all arities
- [ ] Comprehensive testing for each arity

### Phase 4: Advanced Features
- [ ] Async support
- [ ] Single row queries
- [ ] Unbuffered queries
- [ ] Advanced splitOn patterns

### Phase 5: Polish
- [ ] Performance optimization
- [ ] Error messages
- [ ] Documentation
- [ ] Examples

## Open Questions

1. Should we support splitOn with regex patterns?
2. How to handle case sensitivity in column matching?
3. Should we support custom split logic via attributes?
4. What's the behavior with duplicate column names?

## References

- Dapper multi-mapping documentation: https://dappertutorial.net/result-multi-mapping
- Original Dapper splitOn implementation
- Query<T1, T2, ..., TReturn> method signatures in Dapper 2.1.66

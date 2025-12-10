using Dapper;
using System.Data.Common;

[module: DapperAot]

public static class Foo
{
    static void SomeCode(DbConnection connection)
    {
        // Two-type query with default splitOn
        _ = connection.Query<Product, Category, Product>(
            "SELECT * FROM Products p INNER JOIN Categories c ON p.CategoryId = c.Id",
            (product, category) => { product.Category = category; return product; });

        // Two-type query with explicit splitOn
        _ = connection.Query<Product, Category, Product>(
            "SELECT * FROM Products p INNER JOIN Categories c ON p.CategoryId = c.Id",
            (product, category) => { product.Category = category; return product; },
            splitOn: "Id");
    }

    public class Product
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int CategoryId { get; set; }
        public Category Category { get; set; }
    }

    public class Category
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }
}

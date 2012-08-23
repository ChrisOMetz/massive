using System;
using System.Dynamic;

namespace Massive.SQLServer2012
{
    /// <summary>
    /// A class that wraps your database table in Dynamic Funtime
    /// </summary>
    public class DynamicModel : Massive.DynamicModel
    {
        public DynamicModel(string connectionStringName, string tableName = "", string primaryKeyField = "", string descriptorField = "", bool primaryKeyIsIdentity = false)
            : base(connectionStringName, tableName, primaryKeyField, descriptorField, primaryKeyIsIdentity)
        {
        }

        /// <summary>
        /// Returns a dynamic PagedResult. Result properties are Items, TotalPages, and TotalRecords.
        /// </summary>
        public override dynamic Paged(string where = "", string orderBy = "", string columns = "*", int pageSize = 20, int currentPage = 1, params object[] args)
        {
            return BuildPagedResultForSqlServer2012AndAzure(where: where, orderBy: orderBy, columns: columns, pageSize: pageSize, currentPage: currentPage, args: args);
        }

        public override dynamic Paged(string sql, string primaryKey, string where = "", string orderBy = "", string columns = "*", int pageSize = 20, int currentPage = 1, params object[] args)
        {
            return BuildPagedResultForSqlServer2012AndAzure(sql, primaryKey, where, orderBy, columns, pageSize, currentPage, args);
        }

        private dynamic BuildPagedResultForSqlServer2012AndAzure(string sql = "", string primaryKeyField = "", string where = "", string orderBy = "", string columns = "*", int pageSize = 20, int currentPage = 1, params object[] args)
        {
            dynamic result = new ExpandoObject();
            string countSQL = !string.IsNullOrEmpty(sql) ? string.Format("SELECT COUNT({0}) FROM ({1}) AS PagedTable", primaryKeyField, sql) : string.Format("SELECT COUNT({0}) FROM {1}", PrimaryKeyField, TableName);

            if (String.IsNullOrEmpty(orderBy))
            {
                //orderBy = string.IsNullOrEmpty(primaryKeyField) ? " ORDER BY " + PrimaryKeyField : " ORDER BY " + primaryKeyField;
                orderBy = PrimaryKeyField;
            }

            if (!string.IsNullOrEmpty(where))
            {
                if (!where.Trim().StartsWith("where", StringComparison.CurrentCultureIgnoreCase))
                {
                    where = " WHERE " + where;
                }
            }

            string query = !string.IsNullOrEmpty(sql) ? string.Format("{0} {1} ORDER BY {2} ", sql, @where, orderBy) : string.Format("SELECT {0} FROM {1} {2} ORDER BY {3} ", columns, TableName, @where, orderBy);

            var pageStart = (currentPage - 1) * pageSize;
            query += string.Format(" OFFSET {0} ROWS FETCH NEXT {1} ROWS ONLY", pageStart, pageSize);
            countSQL += where;
            result.TotalRecords = Scalar(countSQL, args);
            result.TotalPages = result.TotalRecords / pageSize;
            if (result.TotalRecords % pageSize > 0)
                result.TotalPages += 1;
            result.Items = Query(string.Format(query, columns, TableName), args);
            return result;
        }

    }
}
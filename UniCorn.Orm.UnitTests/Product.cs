namespace UniCorn.Orm.UnitTests
{
    public class Product : UniCorn.Core.EntityBase
    {
        private System.Int64 _ProductID;
        public System.Int64 ProductID { get { return _ProductID; } set { base.SetField(ref _ProductID, value); } }
        private System.String _ProductName;
        public System.String ProductName { get { return _ProductName; } set { base.SetField(ref _ProductName, value); } }
        private System.Int64 _SupplierID;
        public System.Int64 SupplierID { get { return _SupplierID; } set { base.SetField(ref _SupplierID, value); } }
        private System.Int64 _CategoryID;
        public System.Int64 CategoryID { get { return _CategoryID; } set { base.SetField(ref _CategoryID, value); } }
        private System.String _QuantityPerUnit;
        public System.String QuantityPerUnit { get { return _QuantityPerUnit; } set { base.SetField(ref _QuantityPerUnit, value); } }
        private System.Double _UnitPrice;
        public System.Double UnitPrice { get { return _UnitPrice; } set { base.SetField(ref _UnitPrice, value); } }
        private System.Int64 _UnitsInStock;
        public System.Int64 UnitsInStock { get { return _UnitsInStock; } set { base.SetField(ref _UnitsInStock, value); } }
        private System.Int64 _UnitsOnOrder;
        public System.Int64 UnitsOnOrder { get { return _UnitsOnOrder; } set { base.SetField(ref _UnitsOnOrder, value); } }
        private System.Int64 _ReorderLevel;
        public System.Int64 ReorderLevel { get { return _ReorderLevel; } set { base.SetField(ref _ReorderLevel, value); } }
        private System.String _Discontinued;
        public System.String Discontinued { get { return _Discontinued; } set { base.SetField(ref _Discontinued, value); } }
        public object this[string propertyName]
        {
            get
            {
                if (propertyName == "ProductID")
                    return "ProductID";
                else if (propertyName == "ProductName")
                    return "ProductName";
                else if (propertyName == "SupplierID")
                    return "SupplierID";
                else if (propertyName == "CategoryID")
                    return "CategoryID";
                else if (propertyName == "QuantityPerUnit")
                    return "QuantityPerUnit";
                else if (propertyName == "UnitPrice")
                    return "UnitPrice";
                else if (propertyName == "UnitsInStock")
                    return "UnitsInStock";
                else if (propertyName == "UnitsOnOrder")
                    return "UnitsOnOrder";
                else if (propertyName == "ReorderLevel")
                    return "ReorderLevel";
                else if (propertyName == "Discontinued")
                    return "Discontinued";
                return null;
            }
            set
            {
                if (propertyName == "ProductID")
                    ProductID = (System.Int64)value;
                else if (propertyName == "ProductName")
                    ProductName = (System.String)value;
                else if (propertyName == "SupplierID")
                    SupplierID = (System.Int64)value;
                else if (propertyName == "CategoryID")
                    CategoryID = (System.Int64)value;
                else if (propertyName == "QuantityPerUnit")
                    QuantityPerUnit = (System.String)value;
                else if (propertyName == "UnitPrice")
                    UnitPrice = (System.Int64)value;
                else if (propertyName == "UnitsInStock")
                    UnitsInStock = (System.Int64)value;
                else if (propertyName == "UnitsOnOrder")
                    UnitsOnOrder = (System.Int64)value;
                else if (propertyName == "ReorderLevel")
                    ReorderLevel = (System.Int64)value;
                else if (propertyName == "Discontinued")
                    Discontinued = (System.String)value;
            }
        }
    }
}
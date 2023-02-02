/**
 * Delete data extension
 * https://salesforce.stackexchange.com/questions/381054/how-to-delete-a-data-extension-entire-de-not-only-records-using-ssjs
 *
 * Use between tags <script runat="server"></script>
 */


Platform.Load("core", "1.1.1");

var api = new Script.Util.WSProxy();

try {
    var dataExtensionName = "Customers";
    var req = api.retrieve("DataExtension", ["CustomerKey"], {
        Property: "Name",
        SimpleOperator: "equals",
        Value: dataExtensionName
    });
    var customerKey = req.Results[0].CustomerKey;
    var req = api.retrieve("DataExtension", ["ObjectID"], {
        Property: "DataExtension.CustomerKey",
        SimpleOperator: "equals",
        Value: customerKey
    });

    var objectId = req.Results[0].ObjectID;

    var result = api.deleteItem("DataExtension", { "ObjectID": objectId });

    Write(Stringify(result));

} catch (error) {
    Write(Stringify(error));
}
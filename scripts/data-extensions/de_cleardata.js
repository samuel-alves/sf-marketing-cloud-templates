/**
 * Delete data extension records
 * https://www.linkedin.com/pulse/ssjs-clear-entire-data-extension-sascha-huwald/
 *
 * Use between tags <script runat="server"></script>
 */


// check if needed
// Platform.Load("core", "1.1.1");

var prox = new Script.Util.WSProxy();
var action = "ClearData";
var props = { CustomerKey: 'xxx-xxx-xxx-xxx' };
var data = prox.performItem("DataExtension", props, action);
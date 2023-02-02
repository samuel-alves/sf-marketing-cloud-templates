/**
 * Delete data extension records
 *
 * Snippet from:
 * https://ampscript.xyz/how-tos/how-to-filter-and-delete-multiple-data-extension-records-with-a-script-activity/
 *
 * Use between tags <script runat="server"></script>
 */

Platform.Load("Core", "1.1.5");

try {
    // DataExtension to be cleared
    var customerKey = 'XXXX-XXXX-XXXX-XXXX',
        name = 'DemoDataExtension';

    // init WSProxy
    var api = new Script.Util.WSProxy();


    // QueryDefinition result data
    var res = { Status: '', de: { fieldNames: [], Name: name, CustomerKey: customerKey }, qd: {} },
        de = res.de,
        qd = res.qd;

    // returns true / false
    res.Status = (clearDataExtension()) ? 'OK' : 'Error';

    // Output result
    Write(Stringify(res));
} catch (e) {
    Write(Stringify(e));
}

/**
 * Clear all records in a DataExtension.
 *
 * For fastes processing time, this process creates
 * a SQL query, overwirte the DataExtension and
 * remove the query activity.
 *
 * @param {string}  [fromTableName]     An alternative FROM table for the SQL query.
 *
 * @returns {boolean}
 */
function clearDataExtension(fromTableName) {
    var queryFrom = (fromTableName) ? fromTableName : '_Click',
        querySelectNames = [];

    // retireve relevant DataExtensionFields
    var fields = retrieveDataExtensionFields(["Name", "IsRequired", "IsPrimaryKey"]);
    // no fields found
    if (fields === null) {
        return false;
    }

    // find all IsRequired and IsPrimaryKey and build SelectNames
    for (var i = 0; i < fields.length; i++) {
        if (fields[i].IsPrimaryKey == true || fields[i].IsRequired === true) {
            querySelectNames.push('1 AS [' + fields[i].Name + ']');
        }
        // push names to result object
        res.de.fieldNames.push(fields[i].Name);
    }
    // build select statement to clear the DE
    var queryText = 'SELECT TOP 1 ' + querySelectNames.join(',') + ' FROM ' + queryFrom + ' WHERE 1 = 2';

    // Optional: check if DE has records. Otherwise no need to clean it:
    if (isDataExtensionEmpty()) {
        return true;
    }

    // QueryDefinition creation failed.
    if (!createQueryDefinition(queryText)) {
        return false;
    }

    // execute QueryDefinition
    req = api.performItem("QueryDefinition", { ObjectID: qd.ObjectId }, "Start");
    // QueryDefinition triggered failed
    if (req.Status != 'OK' && req.Results.length <= 0) {
        return false;
    }
    qd.Task = req.Results[0].Task;

    /**
     * Listen to the QueryDefinitionStatus
     * Depending on the SQL queue, this can take a while
     * and may timeout.
     * An AJAX request to a GET handler can also be introduced
     * to overcome this limitation.
     */
    if (isQueryDefinitionComplete(qd.Task.ID) !== true) {
        Write('Query failed to execute');
        // ... do something else if needed
    }

    // regardless of outcome - clean up the temporary queryActivity
    req = api.deleteItem("QueryDefinition", { "ObjectID": qd.ObjectId });
    if (req.Status != 'OK' || req.Results.length <= 0) {
        Write('Delete QueryDefinition failed');
        // ... do something else if needed
    }

    // final check...
    return isDataExtensionEmpty();
}


/**
 * Retrieve required DataExtension fields.
 *
 * @param {array} cols  Array of columns to retrieve
 *
 * @returns {(null|array)} Array with fields or null
 */
function retrieveDataExtensionFields(cols) {
    var filter = {
        Property: "DataExtension.CustomerKey",
        SimpleOperator: "equals",
        Value: de.CustomerKey
    };


    var req = api.retrieve("DataExtensionField", cols, filter);
    return (req.Status != 'OK' || req.Results.length <= 0) ? null : req.Results;
}




/**
 * Is the DataExtension empty.
 *
 * @returns {boolean}
 */
function isDataExtensionEmpty() {
    req = api.retrieve('DataExtensionObject[' + de.CustomerKey + ']', de.fieldNames);


    if (req.Status == 'OK' && req.Results.length <= 0) {
        return true;
    } else {
        return false;
    }
}




/**
 * Create a QueryDefinition.
 *
 * @param {string} queryText    A valid T-SQL statement
 *
 * @returns {boolean}
 */
function createQueryDefinition(queryText) {
    var date = new Date(),
        dateUTC = date.setHours(date.getHours() + 6),
        utcTimestamp = Math.floor(dateUTC / 1000),
        queryName = 'temp_SSJS_clearDataExtension_' + utcTimestamp;


    // create the QueryDefinition
    var req = api.createItem("QueryDefinition", {
        Name: queryName,
        CustomerKey: Platform.Function.GUID(),
        TargetUpdateType: "Overwrite",
        TargetType: "DE",
        DataExtensionTarget: {
            CustomerKey: de.CustomerKey,
            Name: de.Name
        },
        QueryText: queryText
    });


    if (req.Status != 'OK' && req.Results.length <= 0) {
        return false;
    } else {
        // save the result in the qd object
        qd.Definition = req.Results[0].Object;
        qd.ObjectId = qd.Definition.ObjectID;
        qd.CustomerKey = qd.Definition.CustomerKey;
        return true;
    }
}

/**
 * Check if a QueryDefinition has status complete.
 *
 * @param {integer} taskId  The taskId to verify
 *
 * @returns {boolean}
 */
function isQueryDefinitionComplete(taskId) {
    var complete = false,
        statusPosition = null,
        cols = ["Status"];
    filter = {
        Property: "TaskID",
        SimpleOperator: "equals",
        Value: taskId
    };

    /*
     * The position of the status value in the result object is determine
     * by the position in the request cols array. As cols array can be dynamic,
     * the position value should not be hardcode but programatically retrieved.
     *
     * If Array.indexOf has been added as Polyfill:
     * @example: statusPosition = cols.indexOf['Status']
     *
     * With no Polyfill for Array.indexOf we can iterate through cols.
     *
     * NOTE: The native SSJS 'indexOf' is String.indexOf not Array.indexOf.
     * Both have different purpose, and return a different result.
     */
    for (var i = 0; i < cols.length; i++) {
        if (cols[i] == 'Status') {
            statusPosition = i;
            break;
        }
        // No Status is set in the cols array. We cannot proceed
        return false;
    }

    // listen to status change
    while (!complete) {
        complete = true;

        // retrieve the status
        var req = api.retrieve("AsyncActivityStatus", cols, filter),
            status = req.Results[0].Properties[statusPosition].Value;

        // keep listen, or status is compelte return true, otherwise status unknown / retrieve failed retrun false
        if (status == 'Queued' || status == 'Processing') {
            complete = false;
        } else if (status == 'Complete') {
            return true;
        } else {
            return false;
        }
    }
}
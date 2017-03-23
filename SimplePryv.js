// Example of Standard Connections in Web Data Connectors using JSONPlaceholder JSON endpoints
// Tableau 10.1 - WDC API v2.1

var pyRoot = "https://perki.pryv.me/";
var pyAuth = "/?auth=cj0kvecn50mu577pnse7amo2w";

var pyConnection = null;

// Define our Web Data Connector
(function(){
  var myConnector = tableau.makeConnector();



  // Define the schema
  myConnector.getSchema = function(schemaCallback) {

    var event_num_cols = [{
      id: "id",
      dataType: tableau.dataTypeEnum.string
    }, {
      id: "streamId",
      alias: "streamId",
      dataType: tableau.dataTypeEnum.string,
      foreignKey: {tableId: 'stream', columnId: 'id'}
    }, {
      id: "time",
      alias: "time",
      dataType: tableau.dataTypeEnum.datetime
    }, {
      id: "duration",
      alias: "duration",
      dataType: tableau.dataTypeEnum.float
    }, {
      id: "type",
      alias: "type",
      dataType: tableau.dataTypeEnum.string
    }, {
      id: "content",
      alias: "content",
      dataType: tableau.dataTypeEnum.float,
      columnRole: tableau.columnRoleEnum.measure
    }];

    var eventTable = {
      id: "eventNum",
      alias: "Numerical Events",
      columns: event_num_cols
    };

    // Schema for time and URL data
    var stream_cols = [{
      id: "id",
      dataType: tableau.dataTypeEnum.string
    }, {
      id: "name",
      alias: "name",
      dataType: tableau.dataTypeEnum.string
    }, {
      id: "parentId",
      alias: "parentId",
      dataType: tableau.dataTypeEnum.string,
      foreignKey: {tableId: 'stream', columnId: 'id'}
    }];

    var streamTable = {
      id: "stream",
      alias: "Streams table",
      columns: stream_cols
    };
    schemaCallback([streamTable, eventTable]);
  };


  myConnector.getData = function(table, doneCallback) {
    // Load our data from the API. Multiple tables for WDC work by calling getData multiple times with a different id
    // so we want to make sure we are getting the correct table data per getData call
    switch (table.tableInfo.id) {
      case 'stream':
        getStreams(table, doneCallback);
        break;
      case 'eventNum':
        getNumEvents(table, doneCallback);
        break;
    }


  }


  tableau.registerConnector(myConnector);
})();

function dateFormat(time) {
  var d = new Date(time);
   return [d.getMonth()+1,
      d.getDate(),
      d.getFullYear()].join('/')+' '+
      [d.getHours(),
        d.getMinutes(),
        d.getSeconds()].join(':');
}


function getNumEvents(table, doneCallback) {
  var filter = new pryv.Filter({limit : 200});
  var tableData = [];
  pyConnection.events.get(filter, function (err, events) {
    for (var i = 0; i < events.length; i++) {
      var event = events[i];
      if (pryv.eventTypes.isNumerical(event.type)) {
        tableData.push({
          id: event.id,
          streamId: event.streamId,
          type: event.type,
          content: event.content,
          time: dateFormat(event.timeLT),
          duration: event.duration
        });
      }
    }
    // Once we have all the data parsed, we send it to the Tableau table object
    table.appendRows(tableData);
    doneCallback();

  });
}

function getStreams(table, doneCallback) {
  pyConnection.streams.get(null, function(err, streams) {
    function addChilds(tableD, streamArray) {
      for (var i = 0; i < streamArray.length; i++) {
        var stream = streamArray[i];
        tableD.push(
          {
            id: stream.id,
            parentId: stream.parentId,
            name: stream.name
          }
        );
        addChilds(tableD, stream.children);
      }
    }

    var tableData = [];
    addChilds(tableData, streams);
    // Once we have all the data parsed, we send it to the Tableau table object
    table.appendRows(tableData);
    doneCallback();
  });
}


// Helper function that loads a json and a callback to call once that file is loaded

function loadJSON(url, cb) {
  var obj = new XMLHttpRequest();
  obj.overrideMimeType("application/json");
  obj.open("GET", url, true);

  obj.onreadystatechange = function() {
    if (obj.readyState == 4 && obj.status == "200"){
      cb(obj.responseText);
    }
  }
  obj.send(null);
}

function send() {

}





// UI

// force usage of staging servers
// comment or remove for use on staging or
// custom infrastructure
//
//pryv.Auth.config.registerURL = {
//    host: 'reg.pryv.in',
//        'ssl': true
//};

// Authenticate user

var authSettings = {
  requestingAppId: 'tableau-demo',
  requestedPermissions: [
    {
      streamId: '*',
      level: 'read'
    }
  ],
  // set this if you don't want a popup
  returnURL: 'self#',
  // use the built-in auth button (optional)
  spanButtonID: 'pryv-button',
  callbacks: {
    initialization: function () { },
    // optional; triggered if the user isn't signed-in yet
    needSignin: function(popupUrl, pollUrl, pollRateMs) { },
    needValidation: null,
    signedIn: onSignedIn,
    refused: function (reason) { },
    error: function (code, message) { }
  }
};

pryv.Auth.setup(authSettings);



/**
 * @param {Pryv.Connection} connection
 * @param {string} langCode
 */
function onSignedIn(connection, langCode) {
  console.log('Signed in!');
  pyConnection =  connection;

}


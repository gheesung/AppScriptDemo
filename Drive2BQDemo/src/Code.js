/*
  Drive to Big Query Demo - 
  Demostrate how to read a CSV file from Google Drive and import into 
  Big Query
  
  Copyright (C) 2017  Ghee Sung
  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.
  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.
  You should have received a copy of the GNU General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

var gprojectId = 'gsinformationsystem';
var datasetID = 'Drive2BQDemo';

var CSVFolderID = '1PCpm6l8q-Ekccln35-EJ5OQaEaXubl4Y';
var DoneFolderID = '1plgPhpul41q_I_tcX5W-hZq3SW_83iP1';
var ErrorFolderID = '1TBfu_9YHpQQXnf2U2DDh-PaNq_d3s7SY';

var sample_schema = [
  { "name": "Outlet", "type": "STRING" },
  { "name": "Date","type": "STRING" },
  { "name": "Time", "type": "STRING" },
  { "name": "Ref1","type": "STRING" },
  { "name": "Ref2","type": "STRING" },
  { "name": "Disc", "type": "STRING" },
  { "name": "ServiceCharge", "type": "STRING" },
  { "name": "OtherCharge","type": "STRING" },
  { "name": "Total", "type": "STRING" }
]; 

function uploadFileToBigQuery(){
  var fileArry = getFileinFolder(CSVFolderID);
  
  var fileid;
  var filename;
  var fileobj;
  var err=0;
  if (fileArry.length == 0){
    Logger.log("uploadFileToBigQuery: No file found in directory");
    return;
  }
  for (var i=0; i<fileArry.length;i++){
    Logger.log(JSON.stringify(fileArry[i]));

    fileid = fileArry[i][1];
    filename = fileArry[i][0];
      
    // the file name must be sample_yyyymmdd.csv
    if (('csv' == right(filename,3) ) && ('sample_' == left(filename,7) )){
      
      fileobj = DriveApp.getFileById(fileid);
      Logger.log("uploadFileToBigQuery: Importing " + filename);
      // Load CSV data from Drive and convert to the correct format for upload.
      var data = fileobj.getBlob().setContentType('application/octet-stream');
      var tableid = left(filename,15);
      
      try{
        createBQTable(datasetID, tableid, sample_schema);
  
        // Create the data upload job.
        var job = {
          configuration: {
            load: {
              destinationTable: {
                projectId: gprojectId,
                datasetId: datasetID,
                tableId: tableid
              },
              skipLeadingRows: 1,
              writeDisposition: "WRITE_TRUNCATE"
            }
          }
        };
  
        job = BigQuery.Jobs.insert(job, gprojectId, data);
        
        moveFileToFolder(fileobj, DoneFolderID, 'done');
      } 
      catch(e){
        Logger.log(e);
        err = 1;
        moveFileToFolder(fileobj, ErrorFolderID, 'error');
      }
      
    }
    
  }

}

// create BQ table
function createBQTable(datasetid, tableid, schema) {
  // Replace this value with the project ID listed in the Google
  // Create a dataset in the BigQuery UI (https://bigquery.cloud.google.com)
  // and enter its ID below.
  Logger.log(gprojectId);

  // Create the table.
  var table = {
    tableReference: {
      projectId: gprojectId,
      datasetId: datasetid,
      tableId: tableid
    },
    schema: {
      fields: schema
    }
  };
  try{
    table = BigQuery.Tables.insert(table, gprojectId, datasetid);
  }
  catch ( e){
    Logger.log('Error Create Table: %s', table.id);
    Logger.log(e);
    throw(e);
  }
  Logger.log('Table created: %s', table.id);
  return table;

} 

// move file to another folder
function moveFileToFolder(fileobj, folderid, type){
  if (type == 'error'){
    
    fileobj.makeCopy(DriveApp.getFolderById(folderid));
  }
  else{
    fileobj.makeCopy(DriveApp.getFolderById(folderid));
  }
  fileobj.setTrashed(true);
}

// get all the files in a folder
function getFileinFolder(folderid) {
 
  var folder = DriveApp.getFolderById(folderid);
  var files = folder.getFiles();
  var output = [];
  while (files.hasNext()) {
    var file = files.next();
    var name = file.getName();
    var type = file.getMimeType();
    var url = file.getUrl();
    var id = file.getId();
    //Logger.log(file.getName());
    output.push([name, id, type, url, folder]);
  }
  return output;
}

// get the rightmost x characters
function right(str,chr){
  return newstr=str.substr(str.length-chr,str.length)
}

// get the leftmost x characters
function left(str,chr){
  return newstr=str.substr(0,chr)
}
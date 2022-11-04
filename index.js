console.time('time'); //Starting time countdown

var fs = require('fs'),
    split = require('split');
  
var stream = fs.createReadStream(process.argv[2], {flags: 'r', encoding: 'utf-8'});      //Creating a read only stream for the JSON file
const wmap = new Map();


//Function taking the parsed data and the filePrefix to name the files accordingly
function Nestloop(json,filePrefix){
    for (var key in json) {         //Looping through all the objects in the Parsed JSON data of a single line

        //to make the null,undefined data create an empty line in the file
        if(json[key] == null || json[key] == undefined){  
            json[key]="";
        }

        //if else statement used for desired file prefix
        if(filePrefix == ""){       
            var dot = '';
        }
        else{
            dot = '.';
        }

        //Condition to go through the nested objects
        if (typeof json[key] === "object") {        
            Nestloop(json[key] , filePrefix + dot + key);     
        } 

        //Creating a map to store new write streams so as to not create it repeatedly
        else{
            if(wmap[filePrefix + dot + key] == undefined){
                wmap[filePrefix + dot + key] = fs.createWriteStream('Output/' + filePrefix + dot + key + '.column.log' , {flags: 'a'});                
            }
            wmap[filePrefix + dot + key].write(json[key].toString()+'\n');
        }
    }
}

//Running the read only stream and using split() to split the JSON file data at EOL and working on one line at a time till EOF
var linestream = stream.pipe(split());

linestream
.on('data', function(chunk){
    //Parsing through the split data given and calling the function
    try{
        var json = JSON.parse(chunk);    
        Nestloop(json,"");     
    }
    catch(e){}            
})
.on('end', function () {
    console.log('Process Completed');
    console.timeEnd('time'); //ending the time countdown and printing the total time taken to run the process
});

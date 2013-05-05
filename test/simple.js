var assert = require("assert");
var Qred = require("../lib/qred");

var tests = [ 
    function singleJob(done) {
        var q = new Qred({
            handler: function(data, callback) {
                callback(null, JSON.stringify(data));
            }
        });
        var data = { data1: "a", data2: "b" };
        q.submitJob("ajobid", data , {}, function(err, result) {
            assert(!err);
            assert(result == JSON.stringify(data));
            done();
        });

    }
];


//Run in domain to catch asserts
function runNextTest() {
    var test = tests.shift();
    if(!test) {
        console.log("Finished tests");
        process.exit(0);
    }
    test(runNextTest);
}

runNextTest();

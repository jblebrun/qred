var assert = require("assert");
var Domain = require("domain");


try {
    var redis = require("redis");
} catch(err) {
    console.log("npm install redis to run tests");
    process.exit(-1);
}

exports.getClient = function() {
    var client;
    try {
        client = redis.createClient(6479);
    } catch(err) {
        console.log("Run a redis server on port 6479");
        process.exit(-1);
    }
    return client;
};

var default_timeout = 5000;

var specified_tests = null;
if(process.env.tests > 0) {
    specified_tests = process.env.tests.split(',');
}


exports.go = function(tests, beforeeach, complete) {
    //Run in domain to catch asserts
    function runNextTest() {
        var tdomain = Domain.create();
        var test = tests.shift();
        if(!test) {
            console.log("Finished tests");
            complete(null);
        }
        var name = test.name || test.test.name;
        if(specified_tests && specified_tests.indexOf(name) < 0) {
            console.log("specified_tests: "+specified_tests.length);
            console.log("Skipping "+name);
            return runNextTest();
        }
        tdomain.on('error', function(err) {
            console.log("*** Test "+test.name+" failed");
            console.log(err.message);
            console.log(err.stack);
            runNextTest();
        });
        tdomain.run(function() {
            var timeout = default_timeout;
            if(test instanceof Function) {
                test = test; 
            } else if (test instanceof Object && test.hasOwnProperty('test')) {
                if(test.hasOwnProperty("timeout")) {
                    timeout = test.timeout; 
                }
                test = test.test;
            } else {
                assert('Test entries should be functions, or objects with a field called "test" that is a function');
            }
            console.log("--- Starting test "+test.name+(timeout?" (Times out in "+timeout+")":""));

            var timeout_id;
            beforeeach(function() {
                test(function() {
                    clearTimeout(timeout_id);
                    console.log("    Test \""+test.name+"\" passed");
                    process.nextTick(runNextTest);
                });
            });
            timeout_id = setTimeout(function() {
                assert(false, test.name+" timed out!");
            }, timeout);
            tdomain.add(timeout_id);
        });
    }

    runNextTest();
};

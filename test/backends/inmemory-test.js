/*global suite, test, suiteSetup, suiteTeardown, setup, Object, Array */

var Backend = require("../../backends/inmemory-test");
var DEBUG = false;

var should = require('should');
suite('InMemoryTest Backend', function () {
    suiteSetup(function (done) {
        done();
    });

    suiteTeardown(function (done) {
        done();
    });

    setup(function (done) {
        done();
    });

    test('should contain a get method', function (done) {
        var b = new Backend();

        var CachePolicy = require("../../cache_policies/static");
        var policy = new CachePolicy({expiryTime: 100, staleTime: 10}); // Set to remove item after 100s, recalculate every 1s

        // First check the data isn't in the backend, and don't claim the lock, as we're not intending to do any work
        b.get("abc", {noLock: true}, function (err, data, status) {
            should.exist(status);
            status.should.have.property("hit", false);
            status.should.have.property("locked", false);
            status.should.have.property("ownLock", false);

            policy.calculate("abc", 10, "fasd", {},{}, function (err, cp) {
                // Now set the data in the cache
                b.store("abc", 123, null,  cp, {}, function (err, success) {
                    // Try a get again, this time claiming the global lock
                    b.get("abc", {}, function (err, data, status) {
                        should.exist(status);
                        status.should.have.property("hit", true);
                        status.should.have.property("locked", false);
                        status.should.have.property("ownLock", false);
                        should.exist(data);
                        data.should.equal(123);
                        done();
                    });
                });
            });
        });
    });

    var sharedTests = require("./common_tests")(DEBUG);
    var runTest = function (testName, tst) {
        test(testName, function (done) {
            var testFn = tst.bind(this);
            var b = new Backend();
            return testFn(b, done)
        });
    }

    for(var testName in sharedTests) {
        runTest(testName, sharedTests[testName]);
    }

});









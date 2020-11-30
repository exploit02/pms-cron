const argv = require("yargs").argv;
const mongodb = require("mongodb").MongoClient;
const moment = require("moment-timezone");
var db = null;

if (argv.NODE_ENV === "dev") {
    require("dotenv-json")();
}

const scheduledJob = () => {
    mongodb
        .connect(process.env.SRV, { useUnifiedTopology: true })
        .then((dbref) => {
            db = dbref.db(process.env.DB_NAME);
            return db
                .collection("upcomingtarrifs")
                .find({ startDate: moment().tz("Asia/Kolkata").startOf("day").toDate() })
                .toArray();
        })
        .then(async (tarrifToUpdate) => {
            if (tarrifToUpdate.length) {
                var bulkInsert = db.collection("tarrifs").initializeUnorderedBulkOp();
                var insertedIds = [];
                var product, vendor;
                tarrifToUpdate.forEach(function (doc) {
                    id = doc._id;
                    product = product;
                    vendor = vendor;
                    // Insert without raising an error for duplicates
                    bulkInsert.find({ product: product, vendor: vendor }).upsert().replaceOne(doc);
                    insertedIds.push(id);
                });
                await bulkInsert.execute();
            }

            return tarrifToUpdate;
        })
        .then(async (tarrifToRemove) => {
            if (tarrifToRemove.length) {
                var bulkRemove = db.collection("upcomingtarrifs").initializeUnorderedBulkOp();
                tarrifToRemove.forEach(function (doc) {
                    bulkRemove.find({ _id: doc._id }).removeOne();
                });
                await bulkRemove.execute();
            }

            return tarrifToRemove;
        })
        .then((data) => {
            // console.log(data);
            console.log(`${data.length} Tarrif Updated`);
        })
        .catch((err) => {
            console.log(err);
            console.log("Error here...........!");
        });
};

if (argv.NODE_ENV === "dev") {
    console.log("running dev");
    scheduledJob();
} else {
    console.log("prod");
    module.exports.main = (event, context, callback) => {
        context.callbackWaitsForEmptyEventLoop = false;
        scheduledJob();
    };
}

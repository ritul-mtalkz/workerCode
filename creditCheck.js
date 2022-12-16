import Redis from "ioredis"
import { KafkaClient, Consumer, Producer } from "kafka-node"

try {
    // Connecting to kafaClient
    const kafaClient = new KafkaClient()

    // Making producer with the help of kafaClient
    const producer = new Producer(kafaClient)

    // Check producer connect or not
    producer.on("error", function (err) {
        console.log("Producer Error: ", err)
    })

    producer.on("ready", async function () {

        // Connecting to redis
        const redis = new Redis()
        redis.on("error", function (err) {
            console.log("Redis Error: ", err)
        })

        const consumer = new Consumer(kafaClient)

        // Check consumer connect or not
        consumer.on("error", function (err) {
            console.log("Consumer Error: ", err)
        })

        consumer.on("message", async function () {
            const payload = JSON.parse(message.value)

            // Extract requestId, apikey, data from payload
            const { requestId, apiKey, data } = payload

            const creditsKey = `CREDS:${apiKey}:${data.channel}`

            // Get the credits of user
            const creditsAvailable = + ((await redis.get(creditsKey)) || 0)

            // Minimum Credits required for request
            const minCredits = 1

            const redisKey = `REQ:${apiKey}:${requestId}`

            // Obtain Costumer from apikey
            const apiData = await redis.get(`APIKEY:${apiKey}`)

            let checkStatus = "accepted"

            // Check the credits of user
            if (minCredits > creditsAvailable) {
                // Reject the request due to insufficient credits
                const response = {
                    requestId,
                    status: "rejected",
                    message: "Insufficient Credits"
                }
                checkStatus = "low_credits"
                redis.set(redisKey, JSON.stringify(response))
            }
            else {
                // Accept the request and push it to valid request queue
                const response = { requestId, status: checkStatus }
                // add queue process...
                let payloads = [
                    {
                        topic: data.integration.provider,
                        messages: JSON.stringify(payload)
                    }
                ]
                producer.send(payloads, (err, message) => {
                    if (err) {
                        console.log(data.integration.provider, err)
                    } else {
                        console.log(data.integration.provider + " Broker Success")
                    }
                })
                // Update the credits and request status
                redis.multi().decr(creditsKey).set(redisKey, JSON.stringify(response)).exec()
            }
        })
    })
} catch (error) {

    console.log("We Got An Error ", error)
}
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import com.amazonaws.services.lambda.runtime.events.{ApplicationLoadBalancerRequestEvent, ApplicationLoadBalancerResponseEvent}
import io.circe.generic.auto._
import io.circe.parser._
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.services.sqs.model.{ReceiveMessageRequest, PurgeQueueRequest, DeleteMessageBatchRequest, DeleteMessageBatchRequestEntry}
import scala.jdk.CollectionConverters._

case class Input(action: String, arn: String, numRecords: Option[Int] = None, receiptHandles: Option[List[String]] = None)

class Entry extends RequestHandler[ApplicationLoadBalancerRequestEvent, ApplicationLoadBalancerResponseEvent] {

  override def handleRequest(input: ApplicationLoadBalancerRequestEvent, context: Context): ApplicationLoadBalancerResponseEvent = {
    val logger = context.getLogger

    val response = new ApplicationLoadBalancerResponseEvent()
    response.setStatusCode(200)
    response.setHeaders(Map("Content-Type" -> "application/json").asJava)

    // Parse the JSON body
    val parsedInput = decode[Input](input.getBody)

    parsedInput match {
      case Right(request) =>
        val sqsClient = AmazonSQSClientBuilder.defaultClient()

        request.action match {
          case "POLL" =>
            val numRecords = request.numRecords.getOrElse(10)

            // Poll the DLQ
            val receiveRequest = new ReceiveMessageRequest()
              .withQueueUrl(request.arn)
              .withMaxNumberOfMessages(numRecords)
              .withWaitTimeSeconds(5)

            val messages = sqsClient.receiveMessage(receiveRequest).getMessages.asScala
            logger.log(s"Polled ${messages.size} messages from DLQ")

            // Log and return the messages
            val messageBodies = messages.map(_.getBody).mkString(",")
            logger.log(s"Messages: $messageBodies")

            response.setStatusCode(200)
            response.setBody(s"""{"messages": "$messageBodies"}""")

          case "PURGE" =>
            // Purge the DLQ
            val purgeRequest = new PurgeQueueRequest().withQueueUrl(request.arn)
            sqsClient.purgeQueue(purgeRequest)
            logger.log("DLQ Purged")

            response.setStatusCode(200)
            response.setBody("""{"status": "DLQ Purged"}""")

          case "REMOVE" =>
            request.receiptHandles match {
              case Some(handles) if handles.nonEmpty =>
                // Prepare and send the batch delete request
                val deleteEntries = handles.map { handle =>
                  new DeleteMessageBatchRequestEntry(handle, handle)
                }

                val deleteRequest = new DeleteMessageBatchRequest()
                  .withQueueUrl(request.arn)
                  .withEntries(deleteEntries.asJava)

                sqsClient.deleteMessageBatch(deleteRequest)
                logger.log(s"Deleted messages with receipt handles: ${handles.mkString(", ")}")

                response.setStatusCode(200)
                response.setBody(s"""{"status": "Messages Deleted", "deletedHandles": "${handles.mkString(", ")}"}""")

              case _ =>
                logger.log("No receipt handles provided for removal")
                response.setStatusCode(400)
                response.setBody("""{"error": "No receipt handles provided for REMOVE action"}""")
            }

          case _ =>
            response.setStatusCode(400)
            response.setBody("""{"error": "Invalid action"}""")
        }

      case Left(error) =>
        logger.log(s"Failed to parse input: $error")
        response.setStatusCode(400)
        response.setBody(s"""{"error": "Invalid input"}""")
    }

    response
  }
}


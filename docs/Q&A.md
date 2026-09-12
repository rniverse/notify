Gaps in the 4 steps:

Q: Idempotency/dedupe. Kafka is at-least-once by default — a consumer restart or rebalance can redeliver. Your consumer needs a stable notification_id (caller-supplied or generated at API time) checked against DB status before sending, or you'll double-email someone.
A: we will generate a notification id before sending to kafka, on consumer receives it, it'll send and then save it notifications table with status `sent`

Q: Partition key. Decide what you key messages on (recipient? notification type?) — affects both ordering guarantees and how many consumers you can run in parallel.
A: recipient can be multiple, let's talk more about this.

Q: Two distinct failure stages, not one. "Kafka inject failed" (never left your API) vs "Kafka send failed" (came out of the consumer but the email provider call failed) need separate status values — you've already implicitly split these, just make sure the schema does too (status + stage_failed), not one generic failed flag.
A: on notification/send
    - send to kafka
        - sent success - do nothing
        - failed - save to notifications table with status pending
    
Q: "Try once" — then what? Worth deciding now: does the cron retry a row every cycle until some max attempt count, then demote to failed_notifications? Or is it literally one extra attempt, ever? The former needs attempt_count/max_attempts columns; the latter doesn't. I'd lean toward attempt-count-with-cap so a blip in the email provider doesn't permanently kill a notification after one bad cron tick.
A: Yes we can add the current_attempt and max_attempts, but we will add expire or send_before date time field, if it's not before that, fail it and move to failed_notifications, that send_before can be empty on empty it won't effect, by default max_attempts is 1.

Q: Provider boundary. aham currently calls resend directly. Since this becomes the one place that sends email, wrap resend's send behind a small internal interface (per the swappable-boundary rule) even though you're only using one provider today — costs nothing now, means swapping/adding a provider later touches one file.
A: Yes aham won't use resend again, aham will be changed to call the notify service. And any swapping or wrapping we will do it in notify.

Q: Nightly metrics cron — define the output now. A row in a notification_metrics table? A log line? An email/Slack alert to you? Worth deciding since it changes what step 3 needs to record (timestamps, error class, not just pass/fail).
A: we will send an email to configured admin in the db or config, only if there are failures. we will save the reasons as well in the failures, maybe we will need to record failures in a separate table and use that. failed notification is more like, notification is moved to failed_notification, failures is more like audit of each attempt when it's failed with reason and that attempt.


- kafka consumer
    - on payload receive
        - sent success - upsert to notifications table with status sent
        - failed - upsert to notifications or failed_notifications table with status failed (we need to work based on that above expires and attempt logic)

- cron to send notifications
    - pull pending records from notifications
    - based on that above expires and attempt logic try sending it, every notification gets 1 attempt only in a cron.

- cron to send metrics
    - pull metrics based on failed audit
    - group them by reason
    - group them by each quarter of the day
    - only send if not 0.

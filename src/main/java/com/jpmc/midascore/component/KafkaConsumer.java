package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    private final DatabaseConduit databaseConduit;
    private final IncentiveService incentiveService;

    public KafkaConsumer(DatabaseConduit databaseConduit, IncentiveService incentiveService) {
        this.databaseConduit = databaseConduit;
        this.incentiveService = incentiveService;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas")
    public void listen(Transaction transaction) {
        UserRecord sender = databaseConduit.getUser(transaction.getSenderId());
        UserRecord recipient = databaseConduit.getUser(transaction.getRecipientId());

        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {
            // 1. Call API to get incentive
            Incentive incentive = incentiveService.getIncentive(transaction);
            float incentiveAmount = incentive.getAmount();

            // 2. Deduct from Sender (Amount only)
            sender.setBalance(sender.getBalance() - transaction.getAmount());

            // 3. Add to Recipient (Amount + Incentive)
            recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

            // 4. Save to DB
            databaseConduit.save(sender);
            databaseConduit.save(recipient);

            // 5. Create Transaction Record (with incentive)
            TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
            databaseConduit.save(record);

            // 6. CHEAT CODE: Find Wilbur's balance for Task 4 Answer
            if (sender.getName().equals("wilbur")) {
                logger.info("*** WILBUR BALANCE: " + sender.getBalance());
            }
            if (recipient.getName().equals("wilbur")) {
                logger.info("*** WILBUR BALANCE: " + recipient.getBalance());
            }
        }
    }
}
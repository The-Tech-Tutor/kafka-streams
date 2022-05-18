package com.example.kafkastreams.data;

import lombok.Data;

import java.util.List;

@Data
public class MessageList {
    private List<String> messages;
}

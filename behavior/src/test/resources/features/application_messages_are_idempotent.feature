Feature: Application messages are idempotent
	An idempotent stream guarantees at-most-once delivery. Duplicate transactions are 
   	prevented; even if it is received multiple times, a transaction is applied only once.
    Application messages are uniquely identified by sequence number within a stream.

  Background:
      Given client and server applications
      And the client establishes a session with the server
      And the client establishes an idempotent flow to server

  Scenario: Client sends first application message
      When the client application sends an application message
      Then the server session accepts message with sequence number 1
      And presents it to its application
      And the server application has received a total of 1 messages
      Then the client closes the session with the server

  Scenario: Client sends two application messages
      When the client application sends an application message
      When the client application sends an application message
      Then the server session accepts message with sequence number 2
      And the server application has received a total of 2 messages
      Then the client closes the session with the server
      
  Scenario: Client sends duplicate application messages
      When the client application sends an application message with sequence number 1
      When the client application sends an application message with sequence number 1
      And the server application has received a total of 1 messages
      Then the client closes the session with the server
                
  Scenario: Server sends NotApplied message when an application message is missed
      When the client application sends an application message with sequence number 1
      When the client application sends an application message with sequence number 3
      Then the client receives a NotApplied message with fromSeqNo 2 and count 1
      And the client application has received a total of 1 messages
      And the server application has received a total of 2 messages
      Then the client closes the session with the server

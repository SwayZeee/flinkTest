package org.example.deserialization;

public class EventMessage {
    public String transactionID;
    public String id;
    public int fieldOne;
    public double fieldTwo;
    public String fieldThree;
    public int fieldFour;
    public double fieldFive;
    public String fieldSix;
    public int fieldSeven;
    public double fieldEight;
    public String fieldNine;
    public int number;

    public EventMessage() {
    }

    public static EventMessage fromString(String s) {
        String[] tokens = s.split(",");
        try {
            EventMessage eventmessage = new EventMessage();
            eventmessage.transactionID = tokens[0];
            eventmessage.id = tokens[1];
            eventmessage.fieldOne = Integer.parseInt(tokens[2]);
            eventmessage.fieldTwo = Double.parseDouble(tokens[3]);
            eventmessage.fieldThree = tokens[4];
            eventmessage.fieldFour = Integer.parseInt(tokens[5]);
            eventmessage.fieldFive = Double.parseDouble(tokens[6]);
            eventmessage.fieldSix = tokens[7];
            eventmessage.fieldSeven = Integer.parseInt(tokens[8]);
            eventmessage.fieldEight = Double.parseDouble(tokens[9]);
            eventmessage.fieldNine = tokens[10];
            eventmessage.number = Integer.parseInt(tokens[11]);
            return eventmessage;
        } catch (Exception e) {
            throw e;
        }
    }
}

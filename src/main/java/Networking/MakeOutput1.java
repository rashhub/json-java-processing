package Networking;

/**
 * Created by Rash on 15-05-2017.
 */
class MakeOutput1 {
    public static void main(String argv[]) throws Exception
    {
        System.out.print("USER jasleen\r\n");
        System.out.println("Command ok");
        System.out.print("uSeR jasleen\r\n");
        System.out.println("Command ok");
        System.out.print("usr jasleen\r\n");
        System.out.println("ERROR -- command");
        System.out.print("USERjasleen\r\n");
        System.out.println("ERROR -- command");
        System.out.print("user\r\n");
        System.out.println("ERROR -- command");
        System.out.print("USER jasl*n\r\n");
        System.out.println("Command ok");
        System.out.print("USER jaslee\u00a5\r\n");
        System.out.println("ERROR -- username");
        System.out.print("USER           jasleen\r\n");
        System.out.println("Command ok");
        System.out.print("USER ja sl een \r\n");
        System.out.println("Command ok");
        System.out.print("PASS 12@456\r\n");
        System.out.println("Command ok");
        System.out.print("PASS 12*456\r\n");
        System.out.println("Command ok");
        System.out.print("TYPE A\r\n");
        System.out.println("Command ok");
        System.out.print("TYPE I\r\n");
        System.out.println("Command ok");
        System.out.print("TYPE B\r\n");
        System.out.println("ERROR -- type-code");
        System.out.print("SYST\r\n");
        System.out.println("Command ok");
        System.out.print("SYST jasleen\r\n");
        System.out.println("ERROR -- CRLF");
    }
}


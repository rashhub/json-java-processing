package Networking;

/**
 * Created by Rash on 15-05-2017.
 */
class MakeInput1 {
    public static void main(String argv[]) throws Exception
    {
        System.out.print("USER jasleen\r\n");
        System.out.print("uSeR jasleen\r\n");
        System.out.print("usr jasleen\r\n");
        System.out.print("USERjasleen\r\n");
        System.out.print("user\r\n");
        System.out.print("USER jasl*n\r\n");
        System.out.print("USER jaslee\u00a5\r\n");
        System.out.print("USER           jasleen\r\n");
        System.out.print("USER ja sl een \r\n");
        System.out.print("PASS 12@456\r\n");
        System.out.print("PASS 12*456\r\n");
        System.out.print("TYPE A\r\n");
        System.out.print("TYPE I\r\n");
        System.out.print("TYPE B\r\n");
        System.out.print("SYST\r\n");
        System.out.print("SYST jasleen\r\n");
    }
}


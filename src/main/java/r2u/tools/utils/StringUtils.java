package r2u.tools.utils;

public class StringUtils {
    public static String removeCharacters(String str, int numCharToRemove) {
        if (str != null && !str.trim().isEmpty()) {
            return str.substring(0, str.length() - numCharToRemove);
        }
        return "";
    }

    //Questa x far la replace dei caratteri speciali a prescindere del OS.
    //Sembrerebbe che va un cazzo, ma chi se ne sbatte i coglioni x zero EUR in busta.
    public static String removeSpecialCharacter(String string, String regex) {
        return string.replaceAll(regex, "");
    }
}

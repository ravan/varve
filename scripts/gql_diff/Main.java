import java.io.IOException;
import java.nio.file.Path;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.BailErrorStrategy;

public final class Main {
    private static final BaseErrorListener THROWING_ERROR_LISTENER =
            new BaseErrorListener() {
                @Override
                public void syntaxError(
                        Recognizer<?, ?> recognizer,
                        Object offendingSymbol,
                        int line,
                        int charPositionInLine,
                        String msg,
                        RecognitionException e) {
                    throw new ParseCancellationException(msg);
                }
            };

    private Main() {}

    public static void main(String[] args) {
        boolean ok = true;
        for (String arg : args) {
            try {
                String verdict = accepts(Path.of(arg)) ? "ACCEPT" : "REJECT";
                System.out.println(arg + "\t" + verdict);
            } catch (IOException e) {
                System.err.println(arg + ": " + e.getMessage());
                ok = false;
            }
        }
        if (!ok) {
            System.exit(1);
        }
    }

    private static boolean accepts(Path path) throws IOException {
        try {
            GQLLexer lexer = new GQLLexer(CharStreams.fromPath(path));
            lexer.removeErrorListeners();
            lexer.addErrorListener(THROWING_ERROR_LISTENER);

            GQLParser parser = new GQLParser(new CommonTokenStream(lexer));
            parser.removeErrorListeners();
            parser.addErrorListener(THROWING_ERROR_LISTENER);
            parser.setErrorHandler(new BailErrorStrategy());
            parser.gqlProgram();
            return true;
        } catch (ParseCancellationException e) {
            return false;
        }
    }
}

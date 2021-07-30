public class Main {
        public static void main(String[] args) throws Exception {
                Step1.main(new String[]{"/in", "/out1"});
                Step2.main(new String[]{"/out1", "/out2"});
                Step3.main(new String[]{"/out2", "/out3"});
                Step4.main(new String[]{"/out3", "/out4"});

        }
}
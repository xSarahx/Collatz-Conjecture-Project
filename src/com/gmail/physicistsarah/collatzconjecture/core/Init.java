/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.gmail.physicistsarah.collatzconjecture.core;

import java.awt.Dimension;
import java.io.IOException;
import java.math.BigInteger;
import javax.swing.JOptionPane;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.stage.Stage;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import net.jcip.annotations.NotThreadSafe;
import org.controlsfx.dialog.DialogStyle;
import org.controlsfx.dialog.Dialogs;

/**
 *
 * @author Sarah Szabo <PhysicistSarah@Gmail.com>
 */
@NotThreadSafe
public class Init {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        //initThreadsSingleProcessing();
        //initNumberChooser();
        //Application.launch(Init.class);
        initProcessingHub();
    }

    private static void initProcessingHub() {
        //ProcessingHub hub = new ProcessingHub(new BigInteger("1"), new BigInteger("4000000"));
        ProcessingHub hub = new ProcessingHub(ProcessingHub.HubNumericalState.WRITE_UNTIL_DISK_FULL);
        hub.init();
        //initNumberChooser();
    }

    private static void initNumberChooser() {

        CollatzSequencer sequencer;
        do {
            String numberChoice = JOptionPane.showInputDialog(null, "Enter a natural number for the sequencer,"
                    + " or exit to exit the sequencer.",
                    "SciLab Collatz Conjecture Sequencer", JOptionPane.QUESTION_MESSAGE);
            if (numberChoice == null || numberChoice.equalsIgnoreCase("Exit")) {
                System.exit(0);
            }
            try {
                sequencer = new CollatzSequencer(new BigInteger(numberChoice), false);
                CollatzSequencer.FinalSequencerReport<? extends Number> report = sequencer.init();
                JTextArea textArea = new JTextArea(report.toString());
                textArea.setLineWrap(true);
                textArea.setWrapStyleWord(true);
                textArea.setEditable(false);
                JScrollPane scrollPane = new JScrollPane(textArea);
                scrollPane.setPreferredSize(new Dimension(1500, 700));
                JOptionPane.showMessageDialog(null, scrollPane, "SciLab Sequencer Results",
                        JOptionPane.INFORMATION_MESSAGE);
                System.err.println(report);
            } catch (NumberFormatException n) {
                JOptionPane.showMessageDialog(null, "The number entered must be a natural number;"
                        + " an integer that is strictly greater than zero", "Error", JOptionPane.ERROR_MESSAGE);
            }
        } while (true);
    }

    public void start(Stage primaryStage) throws Exception {
        Dialogs dialog = Dialogs.create();
        CollatzSequencer sequencer;
        String command = "Exit";
        while (true) {
            try {
                command = "Exit";
                command = dialog.style(DialogStyle.NATIVE).title("Scilab Collatz Conjecture Sequencer").masthead("Data Entry")
                        .message("Enter a natural number for the sequencer, or exit to exit the sequencer.")
                        .owner(primaryStage).showTextInput("9001").orElseThrow(() -> {
                            return new NumberFormatException("The comand was not present");
                        });
                if (command.equalsIgnoreCase("Exit")) {
                    break;
                } else {
                    sequencer = new CollatzSequencer(new BigInteger(command), true);
                    dialog.style(DialogStyle.NATIVE).title("Scilab Collatz Conjecture Sequencer").masthead("Finished Calculations")
                            .message("Sequencer Results: " + sequencer.init()).showInformation();
                }
            } catch (NumberFormatException n) {
                if (command.equalsIgnoreCase("Exit")) {
                    break;
                }
                dialog.style(DialogStyle.NATIVE).title("Scilab Collatz Conjecture Sequencer").masthead("An Error Has Occurred")
                        .message("The number entered must be a natural number; a positive integer.").showError();
            }
        }
        Platform.exit();
    }
}

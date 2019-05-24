package com.spbsu.flamestream.example.bl.text_classifier.model;

import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierInput;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierOutput;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ModelState;

public class ClassifierState implements ClassifierInput, ClassifierOutput {
    private final ModelState state;

    public ClassifierState(ModelState state) {
        this.state = state;
    }

    public ModelState getState() {
        return state;
    }

}

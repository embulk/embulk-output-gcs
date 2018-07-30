package org.embulk.output;

public enum AuthMethod
{
    private_key("private_key"),
    compute_engine("compute_engine"),
    json_key("json_key");

    private final String string;

    AuthMethod(String string)
    {
        this.string = string;
    }

    public String getString()
    {
        return string;
    }
}

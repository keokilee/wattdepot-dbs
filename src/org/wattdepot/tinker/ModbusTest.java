package org.wattdepot.tinker;

import java.net.InetAddress;
import java.net.UnknownHostException;
import net.wimpi.modbus.Modbus;
import net.wimpi.modbus.io.ModbusTCPTransaction;
import net.wimpi.modbus.msg.ExceptionResponse;
import net.wimpi.modbus.msg.ModbusResponse;
import net.wimpi.modbus.msg.ReadMultipleRegistersRequest;
import net.wimpi.modbus.msg.ReadMultipleRegistersResponse;
import net.wimpi.modbus.net.TCPMasterConnection;
import net.wimpi.modbus.util.ModbusUtil;

public class ModbusTest {

  // Note that the Modbus register map in the Shark manual appears to start at 1, while jamod
  // expects it to start at 0, so you must subtract 1 from the register index listed in the manual!
  /** Register index for "Power & Energy Format" */
  public static final int ENERGY_FORMAT_REGISTER = 30006 - 1;
  /** Number of words (registers) that make up "Power & Energy Format" */
  public static final int ENERGY_FORMAT_LENGTH = 1;
  /** Register index for "W-hours, Total" */
  public static final int ENERGY_REGISTER = 1506 - 1;
  /** Number of words (registers) that make up "W-hours, Total" */
  public static final int ENERGY_LENGTH = 2;
  /** Register index for "Watts, 3-Ph total" */
  public static final int POWER_REGISTER = 1018 - 1;
  /** Number of words (registers) that make up "Watts, 3-Ph total" */
  public static final int POWER_LENGTH = 2;

  public static ReadMultipleRegistersResponse readRegisters(InetAddress addr, int port,
      int register, int length) {
    TCPMasterConnection connection = null;
    ModbusTCPTransaction transaction = null; // the transaction
    ReadMultipleRegistersRequest request = null; // the request

    try {
      // Open the connection
      connection = new TCPMasterConnection(addr);
      connection.setPort(port);
      connection.connect();

      // Prepare the request
      request = new ReadMultipleRegistersRequest(register, length);

      // Prepare the transaction
      transaction = new ModbusTCPTransaction(connection);
      transaction.setRequest(request);
      transaction.execute();
      ModbusResponse response = transaction.getResponse();

      // Close the connection
      connection.close();

      if (response instanceof ReadMultipleRegistersResponse) {
        return (ReadMultipleRegistersResponse) response;
      }
      else if (response instanceof ExceptionResponse) {
        System.err.println("Got Modbus exception response, code: "
            + ((ExceptionResponse) response).getExceptionCode());
        return null;
      }
      else {
        System.err.println("Got strange Modbus reply.");
        return null;
      }
    }
    catch (Exception e) {
      System.err.println("Got exception: " + e.getMessage());
      e.printStackTrace();
      return null;
    }
  }

  /**
   * @param args
   * @throws UnknownHostException If there are problems.
   */
  public static void main(String[] args) throws UnknownHostException {

    /* Variables for storing the parameters */
    InetAddress addr = null; // the slave's address
    int port = Modbus.DEFAULT_PORT;
    int numDecimals; // configured in energy format
    double powerMultiplier; // 
    ReadMultipleRegistersResponse response;

    addr = InetAddress.getByName("168.105.132.21");
    System.out.println("Meter IP:" + addr.toString());
    System.out.println("TCP port:" + port);

    response = readRegisters(addr, port, ENERGY_FORMAT_REGISTER, ENERGY_FORMAT_LENGTH);
    if (response != null) {
      int formatBits = response.getRegisterValue(0);
      // From Shark manual, bitmap looks like this ("-" is unused bit apparently):
      // ppppiinn feee-ddd
      //
      // pppp = power scale (0-unit, 3-kilo, 6-mega, 8-auto)
      // ii = power digits after decimal point (0-3),
      // applies only if f=1 and pppp is not auto
      // nn = number of energy digits (5-8 --> 0-3)
      // eee = energy scale (0-unit, 3-kilo, 6-mega)
      // f = decimal point for power
      // (0=data-dependant placement, 1=fixed placement per ii value)
      // ddd = energy digits after decimal point (0-6)

      // Get energy scale by shifting off 4 bits and then mask with 111 binary
      int energyScale = (formatBits >>> 4) & 7;
      System.out.println("energyScale: " + energyScale); // DEBUG
      switch (energyScale) {
      case 0:
        // watts
        powerMultiplier = 1.0;
        break;
      case 3:
        // kilowatts
        powerMultiplier = 1000.0;
        break;
      case 6:
        // megawatts
        powerMultiplier = 1000000.0;
        break;
      default:
        // should never happen, according to manual
        System.err.println("Unknown energy scale from meter, defaulting to kWh");
        powerMultiplier = 1000.0;
      }
      // Get # of energy digits after decimal point by masking with 111 binary
      numDecimals = formatBits & 7;
      System.out.println("numDecimals: " + numDecimals); // DEBUG

      // Get power scale by shifting off 12 bits
      int powerScale = formatBits >>> 12;
      System.out.println("Power scale: " + powerScale);

      // Get power decimal digits by shifting off 10 bits and masking with 11 binary
      int powerDigits = (formatBits >>> 10) & 3;
      System.out.println("Power digits: " + powerDigits);

      // Get power decimal digits by shifting off 10 bits and masking with 11 binary
      int usePowerDigits = (formatBits >>> 7) & 1;
      System.out.println("Use power digits?: " + usePowerDigits);
    }
    else {
      System.err.println("Unable to get energy format from meter, aborting.");
      return;
    }

    // Get energy counters
    response = readRegisters(addr, port, ENERGY_REGISTER, ENERGY_LENGTH);
    // System.out.println("Word count = " + response.getWordCount()); // DEBUG
    // System.out.println("Byte count = " + response.getByteCount()); // DEBUG
    // System.out.println("First register value = " + response.getRegisterValue(0)); // DEBUG
    // System.out.println("Second register value = " + response.getRegisterValue(1)); // DEBUG

    // It seems like there should be a better way to extract the bytes from 2 registers
    byte[] regBytes = new byte[4];
    if (response.getWordCount() == 2) {
      regBytes[0] = response.getRegister(0).toBytes()[0];
      regBytes[1] = response.getRegister(0).toBytes()[1];
      regBytes[2] = response.getRegister(1).toBytes()[0];
      regBytes[3] = response.getRegister(1).toBytes()[1];
    }
    int wattHoursInt = ModbusUtil.registersToInt(regBytes);
    System.out.println("energy total as int = " + wattHoursInt); // DEBUG
    double wattHours = wattHoursInt / (Math.pow(10.0, numDecimals));
    System.out.println("energy total as double = " + wattHours); // DEBUG
    double sensorDataWattHours = wattHours * powerMultiplier;
    System.out.println("Wh total (ready for SensorData) = " + sensorDataWattHours);

    // Get instantaneous power reading
    response = readRegisters(addr, port, POWER_REGISTER, POWER_LENGTH);
    // It seems like there should be a better way to turn 2 registers into an int
    if (response.getWordCount() == 2) {
      regBytes[0] = response.getRegister(0).toBytes()[0];
      regBytes[1] = response.getRegister(0).toBytes()[1];
      regBytes[2] = response.getRegister(1).toBytes()[0];
      regBytes[3] = response.getRegister(1).toBytes()[1];
    }
    float watts = ModbusUtil.registersToFloat(regBytes);
    System.out.println("Instantaneous watts = " + watts);
  }

}

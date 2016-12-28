/*
 * The MIT License
 *
 * (c) 2004-2015. Parallels IP Holdings GmbH. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.parallels.desktopcloud;

import hudson.model.Node;
import hudson.remoting.Channel;
import hudson.security.Permission;
import hudson.slaves.AbstractCloudComputer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import jenkins.security.MasterToSlaveCallable;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;


public class ParallelsDesktopConnectorSlaveComputer extends AbstractCloudComputer<ParallelsDesktopConnectorSlave>
{
	private static final ParallelsLogger LOGGER = ParallelsLogger.getLogger("PDConnectorSlaveComputer");
	private int numSlavesRunning = 0;

	public ParallelsDesktopConnectorSlaveComputer(ParallelsDesktopConnectorSlave slave)
	{
		super(slave);
	}

	private String getVmIPAddress(String vmId) throws Exception
	{
		int TIMEOUT = 180;
		for (int i = 0; i < TIMEOUT; ++i)
		{
			RunVmCallable command = new RunVmCallable("list", "-f", "--json", vmId);
			String callResult = forceGetChannel().call(command);
			LOGGER.log(Level.SEVERE, " - (%d/%d) calling for IP. Result: %s", i, TIMEOUT, callResult);
			JSONArray vms = (JSONArray)JSONSerializer.toJSON(callResult);
			JSONObject vmInfo = vms.getJSONObject(0);
			String ip = vmInfo.getString("ip_configured");
			if (!ip.equals("-"))
				return ip;
			Thread.sleep(1000);
		}
		throw new Exception("Failed to get IP for VM '" + vmId + "'");
	}

	private JSONObject getVMInfo(String vmId) throws Exception
	{
		RunVmCallable command = new RunVmCallable("list", "-i", "--json");
		String callResult = forceGetChannel().call(command);
		JSONArray vms = (JSONArray)JSONSerializer.toJSON(callResult);
		for (int i = 0; i < vms.size(); i++)
		{
			JSONObject vmInfo = vms.getJSONObject(i);
			if (vmId.equals(vmInfo.getString("ID")) || vmId.equals(vmInfo.getString("Name")))
				return vmInfo;
		}
		return null;
	}

	public Node createSlaveOnVM(ParallelsDesktopVM vm) throws Exception
	{
		String vmId = vm.getVmid();
		LOGGER.log(Level.SEVERE, "Waiting for IP...");
		String ip = getVmIPAddress(vmId);
		LOGGER.log(Level.SEVERE, "Got IP address for VM %s: %s", vmId, ip);
		vm.setLauncherIP(ip);
		if (vm.getPostBuildCommand() != null)
		{
			++numSlavesRunning;
			ParallelsDesktopRestartListener.get().setReadyToRestart(false);
		}

		String slaveName = vm.getSlaveName();
		LOGGER.log(Level.FINE, "Starting slave '%s'", slaveName);
		Node n = new ParallelsDesktopVMSlave(vm, this);
		LOGGER.log(Level.SEVERE, "Slave %s provisioned.", slaveName);
		return n;
	}

	public boolean startVM(ParallelsDesktopVM vm)
	{
		String vmId = vm.getVmid();
		LOGGER.log(Level.SEVERE, "Looking for virtual machine '%s'...", vmId);
		try
		{
			JSONObject vmInfo = getVMInfo(vmId);
			if (vmInfo == null)
			{
				LOGGER.log(Level.SEVERE, "Failed to start virtual machine '%s': no such VM", vmId);
				return false;
			}

			String vmStatus = vmInfo.getString("State");
			ParallelsDesktopVM.VMStates state = ParallelsDesktopVM.parseVMState(vmStatus);
			if (state == null)
			{
				LOGGER.log(Level.SEVERE, "Unexpected VM '%s' state: %s", vmId, vmStatus);
				state = ParallelsDesktopVM.VMStates.Suspended;
			}
			if (vm.getPostBuildBehaviorValue() == ParallelsDesktopVM.PostBuildBehaviors.ReturnPrevState)
				vm.setPrevVMState(state);

			if (state != ParallelsDesktopVM.VMStates.Running)
			{
				LOGGER.log(Level.SEVERE, "Starting virtual machine '%s'", vmId);
				RunVmCallable command = new RunVmCallable("start", vmId);
				forceGetChannel().call(command);
			}
			vm.setProvisioned(true);
			return true;
		}
		catch (Exception ex)
		{
			LOGGER.log(Level.SEVERE, "Error: %s\nFailed to start VM '%s'", ex, vmId);
		}
		stopVM(vm);
		return false;
	}

	private void stopVM(ParallelsDesktopVM vm)
	{
		try
		{
			String action = vm.getPostBuildCommand();
			if (action == null)
			{
				LOGGER.log(Level.SEVERE, "Keep running VM %s", vm.getVmid());
				return;
			}
			LOGGER.log(Level.SEVERE, "Post build action for '%s': %s", vm.getVmid(), action);
			RunVmCallable command = new RunVmCallable(action, vm.getVmid());
			String res = forceGetChannel().call(command);
			LOGGER.log(Level.SEVERE, "Result: %s", res);
			if (numSlavesRunning > 0)
				--numSlavesRunning;
			ParallelsDesktopRestartListener.get().setReadyToRestart(numSlavesRunning == 0);
			vm.setProvisioned(false);
		}
		catch (Exception ex)
		{
			LOGGER.log(Level.SEVERE, "Error: %s", ex);
		}
	}

	public void postBuildAction(ParallelsDesktopVM vm)
	{
		stopVM(vm);
	}
	
	public Channel forceGetChannel() throws InterruptedException, ExecutionException
	{
		Channel channel = getChannel();
		if (channel == null)
		{
			connect(true).get();
			channel = getChannel();
		}
		return channel;
	}

	private static final class RunVmCallable extends MasterToSlaveCallable<String, IOException>
	{
		private static final String cmd = "/usr/local/bin/prlctl";
		private final String[] params;
		
		public RunVmCallable(String... params)
		{
			this.params = params;
		}

		@Override
		public String call() throws IOException
		{
			List<String> cmds = new ArrayList<String>();
			cmds.add(cmd);
			cmds.addAll(Arrays.asList(this.params));
			
			LOGGER.log(Level.SEVERE, "Running command:");
			for (String s: cmds)
				LOGGER.log(Level.SEVERE, " [%s]", s);
			ProcessBuilder pb = new ProcessBuilder(cmds);
			pb.redirectErrorStream(true);
			Process pr = pb.start();
			BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
			String line;
			String result = "";
			while ((line = in.readLine()) != null) 
			{
				result += line;
			}
			try
			{
				pr.waitFor();
			}
			catch (InterruptedException ex)
			{
				LOGGER.log(Level.SEVERE, "Error: %s", ex.toString());
			}
			return result;
		}
	}
	
	@Override
	public boolean hasPermission(Permission permission)
	{
		if (permission == CONFIGURE)
			return false;
		return super.hasPermission(permission);
	}
}

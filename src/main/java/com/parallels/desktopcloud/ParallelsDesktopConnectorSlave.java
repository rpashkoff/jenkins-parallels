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

import hudson.model.Descriptor;
import hudson.model.Node.Mode;
import hudson.slaves.ComputerLauncher;
import hudson.slaves.NodeProperty;
import hudson.slaves.RetentionStrategy;
import hudson.Extension;
import hudson.model.Node;
import hudson.model.TaskListener;
import hudson.slaves.AbstractCloudComputer;
import hudson.slaves.AbstractCloudSlave;
import hudson.slaves.EphemeralNode;
import java.io.IOException;
import java.util.ArrayList;
import org.kohsuke.stapler.DataBoundConstructor;


public class ParallelsDesktopConnectorSlave extends AbstractCloudSlave implements EphemeralNode
{
	private final transient ParallelsDesktopCloud owner;
	private final transient boolean useAsBuilder;
	
	@DataBoundConstructor
	public ParallelsDesktopConnectorSlave(ParallelsDesktopCloud owner, String name, String labelString, String remoteFS,
			ComputerLauncher launcher, boolean useAsBuilder)
			throws IOException, Descriptor.FormException
	{
		super(name, "", remoteFS, 1, Mode.NORMAL, labelString, launcher,
				new RetentionStrategy.Always(),
				new ArrayList<NodeProperty<?>>());
		this.owner = owner;
		this.useAsBuilder = useAsBuilder;
	}

	public ParallelsDesktopCloud getOwner()
	{
		return owner;
	}

	@Override
	public AbstractCloudComputer createComputer()
	{
		return new ParallelsDesktopConnectorSlaveComputer(this);
	}

	@Override
	public String getRemoteFS()
	{
		String res = super.getRemoteFS();
		return res;
	}

	@Override
	public Node asNode()
	{
		return this;
	}

	@Override
	protected void _terminate(TaskListener tl) throws IOException, InterruptedException
	{
		owner.connectorTerminated();
	}
	
	@Override
	public Node.Mode getMode()
	{
		return useAsBuilder ? Node.Mode.NORMAL : Node.Mode.EXCLUSIVE;
	}

	@Extension
	public static final class DescriptorImpl extends SlaveDescriptor
	{
		@Override
		public String getDisplayName()
		{
			return "Parallels Desktop connector slave";
		}

		@Override
		public boolean isInstantiable()
		{
			return false;
		}
	}
}

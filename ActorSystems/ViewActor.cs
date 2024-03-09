using ActorSystems.Messages;
using Akka.Actor;
using Akka.Dispatch;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Threading;

namespace ActorSystems
{
    public class ViewActor: ReceiveActor
    {
        Window window;
        Random rnd;
        Thread windowThread;
        WriteableBitmap writable;
        System.Windows.Threading.Dispatcher dispatcher;

        private void NewWindowHandler(object sender, RoutedEventArgs e)
        {
            //var manualResetEvent = new ManualResetEvent(false);
            windowThread = new Thread(new ThreadStart(ThreadStartingPoint));
            windowThread.SetApartmentState(ApartmentState.STA);
            windowThread.IsBackground = true;
            windowThread.Start();
        }

        private void ThreadStartingPoint()
        {
            dispatcher = System.Windows.Threading.Dispatcher.CurrentDispatcher;
            SynchronizationContext.SetSynchronizationContext(new DispatcherSynchronizationContext(dispatcher));
            //manualResetEvent.Set();
            Window tempWindow = new Window();
            window = tempWindow;
            tempWindow.Show();
            tempWindow.Content = SetupImage();

            System.Windows.Threading.Dispatcher.Run();
        }

        private Image SetupImage()
        {
            var image = new System.Windows.Controls.Image();
            RenderOptions.SetBitmapScalingMode(image, BitmapScalingMode.NearestNeighbor);
            RenderOptions.SetEdgeMode(image, EdgeMode.Aliased);

            var writeableBitmap = BitmapFactory.New(1024, 1024);
            writable = writeableBitmap;
            image.Source = writeableBitmap;
            image.Stretch = Stretch.None;
            image.HorizontalAlignment = HorizontalAlignment.Left;
            image.VerticalAlignment = VerticalAlignment.Top;
            return image;
        }

        public ViewActor()
        {
            NewWindowHandler(this, null);

            rnd = new Random();
            Receive<ViewUpdateRequestMessage>(m =>
            {
                //we're going to get a message with a chunk of data - lets just draw that chunk / rect.
                //we can just draw random colors for now.
                
                dispatcher.Invoke(()=>
               {
                   var datalist = m.data as IList;
                   for (int i = 0; i < datalist.Count-4; i=i+4)
                   {
                       var c = Color.FromArgb((byte)((int)datalist[i + 0] & 0x000000FF), (byte)((int)datalist[i+1] & 0x000000FF), (byte)((int)datalist[i + 2] & 0x000000FF), (byte)((int)datalist[i + 3] & 0x000000FF));
                       writable.SetPixel(m.xoff+(i/4), m.yoff, c);
                   }
                   
               });
             
            });
        }
    }
}

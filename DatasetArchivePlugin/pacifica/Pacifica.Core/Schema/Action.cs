using System.Collections.Generic;

namespace Pacifica.Core.Schema
{
    public class Action
    {
        public Action()
        {
            Inputs = new List<Item>();
            Outputs = new List<Item>();
        }

        public List<Item> Inputs { get; private set; }
        public List<Item> Outputs { get; private set; }
    }
}
